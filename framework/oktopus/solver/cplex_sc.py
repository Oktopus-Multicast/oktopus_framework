from base import Solver
from solution import Solution

from collections import defaultdict, OrderedDict
from multiprocessing import cpu_count
import time
import networkx as nx

try:
    # from docplex.cp.model import CpoModel, context
    from docplex.mp.model  import Model
    from docplex.mp.context  import Context
    
except ImportError as ex:
    pass

class CPLEXSCSolver(Solver):
    def __init__(self, app, **kwargs):
        Solver.__init__(self, name='cplex_sc', app=app, **kwargs)

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
        self._nodes_capa = dict()
        self._links = set()
        self._links_capa = defaultdict(int)
        self._links_cost = defaultdict(int)
        self._srcs = defaultdict(int)
        self._dsts = defaultdict(set)
        self._sessions_bandwidth = defaultdict(int)
        self._services_chain = defaultdict(OrderedDict)
        self._services_res = dict()

        for id, node in self.nodes.items():
            self._nodes |= {id}
            self._nodes_capa[id] = defaultdict(int)
            for service, constraint_map in  node.constraint_map.items():
                if 'cpu' not in constraint_map:
                    continue
                self._nodes_capa[id][service] = constraint_map['cpu']

        for link_id, link in self.links.items():
            self._links |= {link_id}
            self._links_capa[link_id] = int(link.cap)
            self._links_cost[link_id] = link.bw_cost

        for t, s_session in enumerate(self.sessions):
            self._trees |= {t}
            print("Session", s_session.addr, t)
            self._srcs[t] = s_session.src
            if isinstance(s_session.dsts, int):
                s_session.dsts = [s_session.dsts]
            self._dsts[t] |= set(s_session.dsts)
            self._sessions_bandwidth[t] = s_session.bw

            self._services_res[t] = defaultdict(int)
            
            services_chain = [('l0', {self._srcs[t]})] # l0 is the source
            for service in s_session.required_services:
                self._services_res[t][service] = s_session.res[service]['cpu']

                nodes = set()
                for node in self.app.get_nodes_by_service(service):
                    nodes |= {node.node_id}
                services_chain.append((service, nodes))
            services_chain.append(('ff', self._dsts[t])) # ff is destination
            self._services_chain[t] = OrderedDict(services_chain)

            self._solution.get_tree(s_session)
        # Toy Example
        # self._nodes = ('s', 'a', 'b', 'c', 'd', 'e', 'd1', 'd2', 'e0', 'b0', 'd3', 'h', 'd4')
        # self._links = {('s', 'h'), ('h', 's'), ('h', 'd4'), ('d4', 'h'), ('s', 'a'), ('a', 's'), ('a', 'b0'), ('b0', 'a'), ('b', 'b0'), ('b0', 'b'), ('a', 'c'), ('c', 'a'), ('b', 'd'), ('d', 'b'),
        #                 ('c', 'e0'), ('e0', 'c'), ('e', 'e0'), ('e0', 'e'), ('d', 'd1'), ('d1', 'd'), ('e', 'd2'), ('d2', 'e'), ('e', 'd1'), ('d1', 'e'), ('d1', 'd2'), ('d2', 'd1'), ('d1', 'd3')}
        # self._srcs = {0:'s', 1:'s'}
        # self._dsts = {0:{'a', 'd2', 'd3'}, 1:{'h'}}
        # self._services_chain = {0:OrderedDict([('l0', {'s'}),  ('f1', {'a', 'b0'}), ('f2', { 'c', 'b'}),  ('f3', {'e0', 'd'}),  ('f4', {'a',  'e'}),  ('ff', self._dsts)]), 1:OrderedDict([('l0', {'s'}),  ('f1', {'a'}), ('f2', { 'c'}),  ('f3', {'e0'}),  ('f4', {  'e'}),  ('ff', self._dsts)])}
        # self._trees = {0, 1}
        # self._nodes_capa = defaultdict()
        # for u in self._nodes:
        #     self._nodes_capa[u] = defaultdict()
        #     for l in self._services_chain[0]:
        #         self._nodes_capa[u][l] = 1
        # self._links_cost = dict()
        # self._links_capa = dict()
        # for u, v in self._links:
        #     self._links_cost[(u, v)] = 1
        #     self._links_capa[(u, v)] = 100
        # self._sessions_bandwidth = {0:1, 1:1}
        # self._services_res = dict()
        # for t in self._trees:
        #     self._services_res[t] = dict()
        #     for l in self._services_chain[t]:
        #         self._services_res[t][l] = 1

        # self._sessions_bandwidth = {0:1}
        # self._services_res = dict()
        # self._services_res[0] = dict()
        # for l in self._services_chain[0]:
        #     self._services_res[0][l] = 1
        # self._nodes = (27, 51, 33, 15, 28, 26, 52, 29, 53, 57)
        # self._links = {(27, 51), (51, 27), (33, 51), (51, 33), (51, 15), (15, 51), (15, 28), (28, 15), (28, 26), (51, 52), (52, 51), (51, 53), (53, 51), (52, 29), (29, 52), (53, 57), (57, 53)}
        # self.s = 27
        # self._dsts = {0:{53, 15, 26, 57, 29}}
        # self._services_chain = OrderedDict([('l0', {27}),  (5, {33, 15}), (6, {33, 15}),  (7, {33, 15}),  ('ff', self._dsts)])


        #-----------------------------------------------------------------------------
        # Prepare the data for modeling
        #-----------------------------------------------------------------------------

        # Set of neighbor nodes
        self.N = defaultdict(set)
        for u, v in self._links:
            self.N[u] |= {v}
            self.N[v] |= {u}


    def optimize(self):
        #-----------------------------------------------------------------------------
        # Build the model
        #-----------------------------------------------------------------------------

        # Create MP model
        mdl = Model()
        # mdl.context.cplex_parameters.threads = 1
        mdl.set_time_limit(60*60*24)


        #########################
        # Variables
        #########################
        # psi
        data_flow = {(i, l, u, v): mdl.binary_var(name="data_flow_{0}_{1}_{2}_{3}".format(i, l, u, v)) for i in self._trees for u, v in self._links for l in self._services_chain[i] }

        # phi
        ser_node_dsts = dict()
        for i in self._trees:
            for u in self._nodes:
                for d in self._dsts[i]:
                    for l, l_set in self._services_chain[i].items():
                        if l == 'ff': # destination
                            if d == u:
                                ser_node_dsts[(i, l, d, u)] = 1 # force to reach destination
                            else:
                                ser_node_dsts[(i, l, d, u)] = 0 
                        else:
                            # check if the service is deploy on node u
                            if u in l_set:
                                ser_node_dsts[(i, l, d, u)] = mdl.binary_var(name="ser_node_dsts_{0}_{1}_{2}_{3}".format(i, l, d, u))
                            else:
                                ser_node_dsts[(i, l, d, u)] = 0 # do not allow traffic if service is not deployed on node u

        # tau
        data_flow_dsts = dict()
        for i in self._trees:
            for u in self._nodes:
                for v in self._nodes:
                    for d in self._dsts[i]:
                        for l in self._services_chain[i]:
                            if (u, v) in self._links:
                                data_flow_dsts[(i, d, l, u, v)] = mdl.binary_var(name="data_flow_dsts_{0}_{1}_{2}_{3}_{4}".format(i, d, l, u, v))                                
                            else:
                                data_flow_dsts[(i, d, l, u, v)] = 0 # do not allow traffic on non-existing links

        # link usage
        link_usage =  {(u, v): mdl.integer_var(0, self._links_capa[(u, v)], name="link_usage_{0}_{1}".format(u, v)) for u, v in self._links}

        # node service usage
        node_service = {(i, l, u): mdl.binary_var(name="node_service_{0}_{1}_{2}".format(i, l, u)) for i in self._trees for u in self._nodes for l in self._nodes_capa[u]}


        #########################
        # Constraints
        #########################

        # traffic to destinations been through source
        for i in self._trees:
            for d in self._dsts[i]:
                ser_node_dsts[(i, 'l0', d, self._srcs[i])]  = 1

        # traffic to destinations been through all services
        for i in self._trees:
            for d in self._dsts[i]:
                for l, l_set in self._services_chain[i].items():
                    if l == 'l0' or l == 'ff':
                        continue
                    mdl.add(mdl.sum(ser_node_dsts[(i, l, d, u)]  for u in l_set) == 1)

        # destination reach all services in order
        for i in self._trees:
            for d in self._dsts[i]:
                for u in self._nodes:
                    for li in range(len(self._services_chain[i])-1):
                        l = list(self._services_chain[i])[li]
                        mdl.add(mdl.sum(data_flow_dsts[(i, d, l, u, v)]for v in self.N[u]) - mdl.sum(data_flow_dsts[(i, d, l, v, u)]for v in self.N[u]) >= ser_node_dsts[(i, l, d, u)] - ser_node_dsts[(i, list(self._services_chain[i])[li+1], d, u)])

        # overlap edge count once (multicast)
        for i in self._trees:
            for l in self._services_chain[i]:
                for d in self._dsts[i]:
                    for u, v in self._links:
                        mdl.add(data_flow[(i, l, u, v)] >= data_flow_dsts[(i, d, l, u, v)])

        # Node service capacity
        for i in self._trees:
            for l, l_set in self._services_chain[i].items():
                if l == 'l0' or l == 'ff':
                    continue
                for u in l_set:
                    for d in self._dsts[i]:
                        mdl.add(node_service[(i, l, u)] >= ser_node_dsts[(i, l, d, u)] )

        for u in self._nodes:
            for l in self._nodes_capa[u]:
                mdl.add(mdl.sum(node_service[(i, l, u)] * self._services_res[i][l]  for i in self._trees ) <= self._nodes_capa[u][l])

        # Edge capacity 
        for u, v in self._links:
            mdl.add(mdl.sum(data_flow[(i, l, u, v)] * self._sessions_bandwidth[i] for i in self._trees for l in self._services_chain[i]) <= link_usage[(u, v)])

        #########################
        # Objective
        #########################
        # 1a Minimize the routing cost
        objective = mdl.sum(data_flow[(i, l, u, v)] * self._links_cost[(u, v)] * self._sessions_bandwidth[i] for i in self._trees for l in self._services_chain[i] for u, v in self._links )
        mdl.minimize(objective)
        mdl.print_information()

        start_time = time.time()

        # Solve model
        print "\nSolving model....", 
        msol = mdl.solve(log_output=True)
        print mdl.solution

        total_time = time.time() - start_time
        self._solution.total_time = total_time

        if not msol:
            print('*** Problem has no solution')
            return self._solution
        else:
            print('>>>> OBJ. VALUE', msol.get_objective_value())
            print("All DONE: True")
            # print mdl.solution

            #session routing + session cost
            sess_link = defaultdict(list)
            sess_cost = defaultdict(float)
            sess_ser_n = dict() # Service node 
            sess_dst_hops = defaultdict(int) # longest hop
            for dvar in mdl.solution.iter_variables():
                if 'data_flow' in dvar.to_string() and 'dsts' not in dvar.to_string():
                    data_flow_var = dvar.to_string().split("_")
                    sess_link[int(data_flow_var[2])].append( (int(data_flow_var[4]), int(data_flow_var[5])) )
                    sess_cost[int(data_flow_var[2])] += self._links_cost[(int(data_flow_var[4]), int(data_flow_var[5]))] * self._sessions_bandwidth[int(data_flow_var[2])]

                if 'ser_node' in dvar.to_string():
                    ser_node_var = dvar.to_string().split("_")
                    if int(ser_node_var[3]) not in sess_ser_n:
                        sess_ser_n[int(ser_node_var[3])] = defaultdict(set)
                    sess_ser_n[int(ser_node_var[3])][ser_node_var[4]].add(int(ser_node_var[6]))

                if 'data_flow' in dvar.to_string() and 'dsts' in dvar.to_string():
                    data_flow_var = dvar.to_string().split("_")
                    sess_dst_hops[(int(data_flow_var[3]), int(data_flow_var[4]))] += 1

            for sid, s_session in enumerate(self.sessions):
                for h, t in sess_link[sid]:
                    self.links[(h, t)].used_cap += s_session.bw * self.links[(h, t)].bw_cost
                self.update_session_cost(solution=self._solution, session=s_session,  links=sess_link[sid])

        return self._solution