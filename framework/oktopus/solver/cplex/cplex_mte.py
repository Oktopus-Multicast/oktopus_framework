from ..base import Solver
from ..solution import Solution

from collections import defaultdict
from multiprocessing import cpu_count
import time
import networkx as nx

try:
    from docplex.mp.model  import Model
    from docplex.mp.context  import Context
except ImportError as ex:
    pass


class CPLEXMTESolver(Solver):
    def __init__(self, app, **kwargs):
        Solver.__init__(self, name='cplex_mte', app=app, **kwargs)

        self._timelimit = self.options.pop('time_limit', 60*60*2)
        self._num_worker = int(self.options.pop('num_worker', cpu_count()))

        self._solution = Solution()

        self.sessions = self.app.get_sessions()
        self.graph = self.app.graph
        self.nodes = self.app.nodes
        self.links = self.app.links

        #-----------------------------------------------------------------------------
        # Initialize the problem data
        #-----------------------------------------------------------------------------

        self._trees = set()
        self._nodes = set()
        self._edges = set()
        self._edges_capa = defaultdict(int)
        self._edges_cost = defaultdict(int)
        self._srcs = defaultdict(int)
        self._dsts = defaultdict(set)
        self._sessions_bandwidth = defaultdict(int)
        self._tcam_cap = defaultdict(int)

        for id, node in self.nodes.items():
            self._nodes |= {id}
            self._tcam_cap[id] = node.services['sdn_router'].resources_cap['tcam']

        for link_id, link in self.links.items():
            self._edges |= {link_id}
            self._edges_capa[link_id] = int(link.cap)
            self._edges_cost[link_id] = link.bw_cost

        for t, s_session in enumerate(self.sessions):
            self._trees |= {t}
            print("Session", s_session.addr, t)
            self._srcs[t] = s_session.src
            self._dsts[t] |= set(s_session.dsts)
            self._sessions_bandwidth[t] = s_session.bw
            self._solution.get_tree(s_session)


        # Toy Example
        # self._trees = {1, 2, 3}
        # self._nodes = (1, 2, 3, 4, 5, 6, 7)
        # self._edges = {(1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7), (7, 1),
        #                   (2, 1), (3, 2), (4 ,3), (5, 4), (6, 5), (7, 6), (1, 7),
        #                   (2, 6), (6, 2)}
        # self._srcs = {1:1, 2:3, 3:4}
        # self._dsts = {1:{6}, 2:{6}, 3:{6}}
        # self._sessions_bandwidth = {1:1, 2:1, 3:1}
        # self._edges_capa = {edge:2 for edge in self._edges}
        # self._edges_cost = {edge:1 for edge in self._edges}
        # self.set_tcam_cap = {u:100 for u in self._nodes}
        # self.set_tcam_cap[2] = 0

        #-----------------------------------------------------------------------------
        # Prepare the data for modeling
        #-----------------------------------------------------------------------------

        # Set of out-neighbor and in-neighbor
        self.N_plus = defaultdict(set)
        self.N_minus = defaultdict(set)
        for u, v in self._edges:
            self.N_plus[u] |= {v}
            self.N_minus[v] |= {u}

        # Data rate
        self.f = {i: self._sessions_bandwidth[i] for i in self._trees}

        # Capacity of e{u, v}
        self.c = {(u, v): self._edges_capa[(u, v)] for u, v in self._edges}

        # Bandwidth cost
        self.k = {(u, v): self._edges_cost[(u, v)] for u, v in self._edges}

        
    def optimize(self):
        #-----------------------------------------------------------------------------
        # Build the model
        #-----------------------------------------------------------------------------

        # Create CPO model
        # mdl = CpoModel()
        # ctx = Context.make_default_context()
        # ctx.solver.agent = 'local'
        # ctx.cplex_parameters.threads = 1
        # print(ctx.solver)
        # ctx.solver.max_threads = 1
        # context.solver.local.execfile = '/rcg/software/Linux/Ubuntu/16.04/amd64/TOOLS/CPLEX/12.8/cpoptimizer/bin/x86-64_linux/cpoptimizer'
        # ctx.solver.local.execfile = '/rcg/software/Linux/Ubuntu/16.04/amd64/TOOLS/CPLEX/12.8/cplex/bin/x86-64_linux/cplex'

        # mdl = Model(context=ctx)
        mdl = Model()
        mdl.context.cplex_parameters.threads = 1
        mdl.set_time_limit(60*60*3)

        #########################
        # Variables
        #########################

        # Binary variable denote if edge e{u, v} is in the path from s{i} to a destination node d in D{i} in T{i}.
        pi = {(i, d, u, v): mdl.binary_var(name="pi_{0}_{1}_{2}_{3}".format(i, d, u, v)) for i in self._trees for d in self._dsts[i] for u, v in self._edges}

        # Integer variable denote the number of times that each packet of T{i} 
        # is sent in edge e{u, v} via multicast (SKIP or unicast tunneling).
        # epsilon = {(i, u, v): mdl.integer_var(0, self.set_edges_capa[(u, v)], name="epsilon_{0}_{1}_{2}".format(i, u, v)) for i in self.set_t  for u, v in self.set_edges}
        epsilon = {(i, u, v): mdl.binary_var(name="epsilon_{0}_{1}_{2}".format(i, u, v)) for i in self._trees  for u, v in self._edges}

        # Binary variable denote if v is a branch state node in T{i}
        # beta = {(i, u): mdl.binary_var(name="beta_{0}_{1}".format(i, u)) for i in self.set_t for u in self.set_nodes}
        # beta = {(i, u): 1 for i in self.set_t for u in self.set_nodes}

        # TCAM
        # tcam = {(i, u): mdl.binary_var(name="tcam_{0}_{1}".format(i, u))  for i in self._trees for u in self._nodes}

        #########################
        # Constraints
        #########################

        # (1) states that the net outgoing flow from s{i} is one, implying that at least one edge e{i,s{i},v} from s{i}
        # to any neighbor node v needs to be selected with pi{i,d,s{i},v} = 1. Note that here decision variables
        # pi{i,d,s{i},v} and pi{i,d,v,s{i}} are two different variables because the flow is directed. 
        # On the other hand, every destination node d is the flow destination.
        for i in self._trees:
            for d in self._dsts[i]:
                mdl.add(mdl.sum(pi[(i, d, self._srcs[i], v)] for v in self.N_plus[self._srcs[i]]) - mdl.sum(pi[(i, d, v, self._srcs[i])] for v in self.N_minus[self._srcs[i]]) == 1)

        # (2)  ensures that the net incoming flow to d is one, implying that at least one edge e{i,u,d} from any neighbor 
        # node u to d must be selected with pi{i,d,u,d} = 1.
        for i in self._trees:
            for d in self._dsts[i]:
                mdl.add(mdl.sum(pi[(i, d, u, d)] for u in self.N_minus[d]) - mdl.sum(pi[(i, d, d, u)] for u in self.N_plus[d]) == 1)

        # (3) guarantees that u is either located in the path or not. If u is located in the path, 
        # both the incoming flow and outgoing flow for u are at least one, indicating that at least 
        # one binary variable pi{i,d,v,u} is 1 for the incoming flow, and at least one binary variable
        # pi{i,d,u,v} is 1 for the outgoing flow. Otherwise, both pi{i,d,v,u} and pi{i,d,u,v} are 0. 
        # Note that the objective function will ensure that pi{i,d,v,u} = 1 
        # for at most one neighbor node v to achieve the minimum bandwidth consumption. 
        # In other words, both the incoming flow and outgoing flow among u and v cannot
        # exceed 1.
        for i in self._trees:
            for d in self._dsts[i]:
                for u in self._nodes:
                    if u != self._srcs[i] and u != d:
                        mdl.add(mdl.sum(pi[(i, d, v, u)] for v in self.N_minus[u]) == mdl.sum(pi[(i, d, u, v)] for v in self.N_plus[u]))

        # (4) states that epsilon{i,u,v} is at least 1 if edge e{u,v} is included in the path from s{i} to at least one d, i.e.,
        # pi{i,d,u,v} = 1. The tree T{i} is the union of the paths from s{i} to all destination nodes in D{i}.
        for u, v in self._edges:
            for i in self._trees:
                for d in self._dsts[i]:
                    mdl.add(pi[(i, d, u, v)] <= epsilon[(i, u, v)])

        # (5) Constraint (5) is the most crucial one. For each node u in T{i} , if it is not a branch state node,
        # i.e., beta{i,u} = 0, u does not maintain a forwarding entry of T{i} in Group Table and 
        # thereby facilitates unicast tunneling. In this case, constraint (5) and the objective function guarantee
        # that the number of packets received from an incoming link e{v,u} must be the summation of the 
        # number of packets sent to every outgoing link e{u,v}. By contrast, when beta{i,u} = 1, constraint (5) becomes 
        # redundant because the Left-Hand-Side(LHS) is smaller than 0 and thereby imposes no restrict on the
        # Right-Hand-Side (RHS)
        # for i in self.set_t:
        #     for u in self.set_nodes:
        #         if u != self.sources[i]:
        #             mdl.add(-(len(self.set_d[i])**2)*beta[(i, u)] + mdl.sum(epsilon[(i, u, v)] for v in self.N_plus[u]) <= mdl.sum(epsilon[(i, v, u)] for v in self.N_minus[u]))

        # SKIP (6) states that each node u can act as a branch state node of at most b{u} trees in T

        # (7) describes that the total multicast bandwidth consumption of in each directed edge e{v,u}
        # cannot exceed c{u,v}.
        for u, v in self._edges:
            mdl.add(mdl.sum(self.f[i] * epsilon[(i, u, v)] for i in self._trees) <= self.c[(u, v)])


        # # TCAM constraint
        # for i in self._trees:
        #     for u, v in self._edges:
        #         mdl.add(tcam[(i, u)] >= epsilon[(i, u, v)])
        #         mdl.add(tcam[(i, v)] >= epsilon[(i, u, v)])

        # for u in self._nodes:
        #     mdl.add(mdl.sum(tcam[(i, u)] for i in self._trees) <= self._tcam_cap[u])

        #########################
        # Objective
        #########################

        objective = mdl.sum(self.f[i]*self.k[(u,v)]*epsilon[(i, u, v)] for i in self._trees for u, v in self._edges )
        # mdl.add(mdl.minimize(objective))
        mdl.minimize(objective)
        mdl.print_information()

        #-----------------------------------------------------------------------------
        # Solve the model and display the result
        #-----------------------------------------------------------------------------
        start_time = time.time()

        # Solve model
        print "\nSolving model....", 
        msol = mdl.solve(log_output=True)

        if not msol:
            print('*** Problem has no solution')
        else:
            print('>>>> OBJ. VALUE', msol.get_objective_value())
            print("All DONE: True")
            print mdl.solution

            ans = defaultdict(list)
            for dvar in mdl.solution.iter_variables():
                if 'epsilon' in dvar.to_string():
                    epsilon_var = dvar.to_string().split("_")
                    ans[int(epsilon_var[1])].append((int(epsilon_var[2]), int(epsilon_var[3])))

            for sid, s_session in enumerate(self.sessions):
                for h, t in ans[sid]:
                    self.links[(h, t)].used_cap += s_session.bw * self.links[(h, t)].bw_cost
                self.update_session_cost(solution=self._solution, session=s_session,  links=ans[sid])




            # for t in self._trees:
            #     # Update framwork tcam
            #     # for u in self.nodes:
            #     #     self.nodes[u].services['sdn_router'].used_cap['tcam'] += msol.solution.get_value(tcam[(t, u)])

            #     # Update links
            #     session_tree = nx.DiGraph() 
            #     for link in ans[t]:
            #         self.links[link].used_cap += self.links[link].bw_cost * self.sessions[t].bw
            #         session_tree.add_edge(link[0], link[1])

            #     map(lambda n: session_tree.add_edge(n, "sink"), self.sessions[t].dsts)
            #     for path in nx.all_simple_paths(session_tree, source=self.sessions[t].src, target="sink"):
            #         self._solution.add_path(self.sessions[t], path[:-1], None)

        total_time = time.time() - start_time
        self._solution.total_time = total_time
        return self._solution