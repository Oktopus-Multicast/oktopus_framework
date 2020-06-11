from base import Solver
from solution import Solution

import time
import networkx as nx
from collections import defaultdict


# from repoze.lru import lru_cache


class MTRSASolver(Solver):
    def __init__(self, app, **kwargs):
        Solver.__init__(self, name='mtrsa', app=app, **kwargs)

        self._solution = Solution()

        self.sessions = self.app.get_sessions()
        self.graph = self.app.graph
        self.nodes = self.app.nodes
        self.links = self.app.links

        self.session_branch_nodes = defaultdict(set)
        self.branch_state_node_sessions = defaultdict(set)
        self.session_branch_state_nodes = defaultdict(set)
        self.sessions_tree = dict()
        self.overloaded_set = set()

        self.node_capacity = defaultdict(set)

        self.beta = 1

    def reassess_tcam_cost(self):
        for node, sessions in self.node_capacity.items():
            self.nodes[node].services['sdn_router'].used_cap['tcam'] = len(sessions)

    # Reassess link usage after branch state allocation
    # def reassess_cost(self):
    #     my_new_dict = defaultdict(int)

    #     for s_idx, s_session in self.sessions_tree.items():
    #         # compute each link usage
    #         a_set = self.session_branch_state_nodes[s_idx]
    #         set_v = set(self.sessions[s_idx].dsts) | a_set
    #         for v in set_v:
    #             while v != self.sessions[s_idx].src:
    #                 head_edge, tail_edge = list(s_session.in_edges(v))[0]
    #                 my_new_dict[(head_edge, tail_edge)] += self.sessions[s_idx].bw
    #                 v = head_edge
    #                 if head_edge in a_set:
    #                     break

    #     for link_id, link in self.links.items():
    #         link.used_cap = my_new_dict[link_id]

    # # Count unicast tunneling
    # def max_count_unicast_tunneling(self):
    #     max_tunnel = 0
    #     reached_from_uni = defaultdict(set)
    #     session_uni_node = defaultdict(int)

    #     for s_idx, s_session in self.sessions_tree.items():
    #         # compute each link usage
    #         a_set = self.session_branch_state_nodes[s_idx]
    #         set_v = set(self.sessions[s_idx].dsts) | a_set
    #         for v in set_v:
    #             original_v = v
    #             unicast_tunnel = False
    #             while v != self.sessions[s_idx].src:
    #                 head_edge, tail_edge = list(s_session.in_edges(v))[0]
    #                 if head_edge in self.session_branch_nodes[s_idx] and head_edge not in self.session_branch_state_nodes[s_idx]:
    #                     unicast_tunnel = True
    #                     break
    #                 v = head_edge
    #                 if head_edge in a_set:
    #                     break
    #             if unicast_tunnel:
    #                 reached_from_uni[s_idx].add(original_v)

    #     for s_idx, s_session in self.sessions_tree.items():
    #         # compute each link usage
    #         a_set = self.session_branch_state_nodes[s_idx]
    #         for v in reached_from_uni[s_idx]:
    #             session_uni_node[v] += 1
    #             if session_uni_node[v] > max_tunnel:
    #                 max_tunnel = session_uni_node[v]
    #             while v != self.sessions[s_idx].src:
    #                 head_edge, tail_edge = list(s_session.in_edges(v))[0]
    #                 session_uni_node[head_edge] += 1
    #                 if session_uni_node[head_edge] > max_tunnel:
    #                     max_tunnel = session_uni_node[head_edge]
    #                 v = head_edge
    #                 if head_edge in a_set:
    #                     break

    #     return max_tunnel

    # Reroute session(s_idx) to offload the node(u)
    def reroute(self, s_idx, u):
        temp_graph = self.graph.copy()

        # Remove in_edge of s_idx tree to prevent rerouting to same path as s_idx tree
        for node in nx.descendants(self.sessions_tree[s_idx], self.sessions[s_idx].src):
            for h, t in self.graph.in_edges(node):
                temp_graph.remove_edge(h, t)

        # Remove edge with insufficient residual capacity
        for link_id in set(self.app.links.items()) & set(temp_graph.edges()):
            if self.links[link_id].cap - (self.links[link_id].used_cap + self.sessions[s_idx].bw) < 0:
                temp_graph.remove_edge(link_id[0], link_id[1])

        # Remove node with insufficient node capacity
        for node_id, node in self.nodes.items():
            if len(self.node_capacity[node_id] | {s_idx}) > node.services['sdn_router'].resources_cap['tcam']:
                temp_graph.remove_node(node_id)

        # search nearby downstream v
        queue = list(self.sessions_tree[s_idx].neighbors(u))
        while len(queue) > 0:
            v = queue.pop(0)
            # If v is a branch node or a destination node
            if v in set(self.session_branch_nodes[s_idx]) | set(self.sessions[s_idx].dsts):

                # check no branch node between u to v
                from_v = v
                while from_v != u:
                    h, t = list(self.sessions_tree[s_idx].in_edges(from_v))[0]
                    from_v = h
                    if from_v in self.session_branch_nodes[s_idx]:
                        break

                if from_v != u:
                    continue
                # Recover in_edge for node v
                for h, t in set(self.graph.in_edges(v)) - set(self.sessions_tree[s_idx].edges()) - set([(t, h) for h, t in self.sessions_tree[s_idx].edges()]):
                    temp_graph.add_edge(h, t, weight=self.graph[h][t]['BandwidthCost'])

                # cost of u to v
                u_v_cost = nx.dijkstra_path_length(self.sessions_tree[s_idx], u, v, weight='weight')
                u_v_path_set = set(nx.dijkstra_path(self.sessions_tree[s_idx], u, v, weight='weight'))

                # find w and a path from w to v
                # Remove descendants to prevent loop
                new_path = None
                for w in nx.descendants(self.sessions_tree[s_idx], self.sessions[s_idx].src) - {v, u} - set(
                        nx.descendants(self.sessions_tree[s_idx], v)):
                    if w in u_v_path_set:
                        continue

                    if len({s_idx} | self.node_capacity[w]) > self.nodes[w].services['sdn_router'].resources_cap[
                        'tcam']:
                        continue

                    # the total cost of the new path is at most l
                    try:
                        # Check node capacity
                        candidate_path = nx.dijkstra_path(temp_graph, w, v, weight='BandwidthCost')

                        for node_id in candidate_path:
                            if len({s_idx} | self.node_capacity[node_id]) > \
                                    self.nodes[node_id].services['sdn_router'].resources_cap['tcam']:
                                continue

                        # Check cost
                        if nx.dijkstra_path_length(temp_graph, w, v, weight='BandwidthCost') * self.sessions[
                            s_idx].bw <= u_v_cost:
                            new_path = candidate_path
                            break
                    except nx.NetworkXNoPath:
                        continue
                if not new_path:
                    continue

                # Remove paths from v to u
                from_v = v
                while from_v != u:
                    h, t = list(self.sessions_tree[s_idx].in_edges(from_v))[0]
                    self.sessions_tree[s_idx].remove_edge(h, t)

                    if len(self.sessions_tree[s_idx].edges(t)) == 0:
                        self.node_capacity[t] -= {s_idx}

                    self.app.links[(h, t)].used_cap -= self.sessions[s_idx].bw  # Remove path usage
                    if len(self.sessions_tree[s_idx].edges(h)) > 0 or h in self.sessions[s_idx].dsts or h == \
                            self.sessions[s_idx].src:
                        break
                    from_v = h

                # Add new paths to the tree
                for i, head_edge in enumerate(new_path[:-1]):
                    tail_edge = new_path[i + 1]
                    self.node_capacity[head_edge] |= {s_idx}
                    self.node_capacity[tail_edge] |= {s_idx}
                    self.sessions_tree[s_idx].add_edge(head_edge, tail_edge,
                                                       weight=self.graph[head_edge][tail_edge]['BandwidthCost'] *
                                                              self.sessions[s_idx].bw)
                    self.app.links[(head_edge, tail_edge)].used_cap += self.sessions[s_idx].bw  # Add path usage
                    # Remove overused link
                    if self.links[(head_edge, tail_edge)].cap - self.links[(head_edge, tail_edge)].used_cap < 0:
                        temp_graph.remove_edge(head_edge, tail_edge)

                    # Update the node usage
                    if len(self.sessions_tree[s_idx].edges(head_edge)) > 1:
                        self.session_branch_nodes[s_idx].add(head_edge)

                    if len(self.sessions_tree[s_idx].edges(tail_edge)) > 1:
                        self.session_branch_nodes[s_idx].add(tail_edge)

            else:
                queue += list(self.sessions_tree[s_idx].neighbors(v))

        # Remove branch to u if necessary
        if len(self.sessions_tree[s_idx].edges(u)) == 0:
            # Remove paths from v to u
            from_u = u
            while from_u not in set(self.sessions[s_idx].dsts) and len(self.sessions_tree[s_idx].edges(from_u)) != 0:
                h, t = list(self.sessions_tree[s_idx].in_edges(from_u))[0]
                self.session_branch_nodes[s_idx] -= {t}
                self.node_capacity[t] -= {s_idx}
                self.sessions_tree[s_idx].remove_edge(h, t)
                self.app.links[(h, t)].used_cap -= self.sessions[s_idx].bw  # Remove path usage
                from_u = h

    # # Reroute u to v from session s_idx
    # def reroute_path(self, s_idx, u, v):
    #     temp_graph = self.graph.copy()
    #     # Remove in_edge of s_idx tree to prevent rerouting to same path as s_idx tree
    #     for node in self.sessions_tree[s_idx].nodes():
    #         if self.sessions_tree[s_idx].in_edges(node) or self.sessions_tree[s_idx].out_edges(node):
    #             for h, t in self.graph.in_edges(node):
    #                 temp_graph.remove_edge(h, t)

    #     # Remove edge with insufficient residual capacity
    #     for link_id in set(self.app.links.items()) & set(temp_graph.edges()):
    #         if self.links[link_id].cap - (self.links[link_id].used_cap + self.sessions[s_idx].bw) < 0:
    #             temp_graph.remove_edge(link_id[0], link_id[1])

    #     # Recover in_edge for node v
    #     for h, t in set(self.graph.in_edges(v))-set(self.sessions_tree[s_idx].edges()):
    #         temp_graph.add_edge(h, t, weight=self.graph[h][t]['BandwidthCost'])

    #     # find w and a path from w to v
    #     # Remove descendants to prevent loop
    #     candidate_w = None
    #     for w in set(self.sessions_tree[s_idx].nodes()) - {v, u} - \
    #             set(nx.descendants(self.sessions_tree[s_idx], v)) - \
    #             set(nx.dijkstra_path(self.sessions_tree[s_idx], u, v, weight='weight')):
    #         # ii) w will not be overloaded after rerouting
    #         if len(self.branch_state_node_sessions[w] - {s_idx}) + 1 > \
    #                 self.nodes[w].services['sdn_router'].resources_cap['tcam'] or \
    #                 not self.sessions_tree[s_idx].in_edges(w):
    #             continue
    #         try:
    #             if nx.dijkstra_path_length(temp_graph, w, v, weight='weight'):
    #                 candidate_w = w
    #                 break
    #         except nx.NetworkXNoPath:
    #             continue
    #     if not candidate_w:
    #         return False

    #     new_path = nx.dijkstra_path(temp_graph, candidate_w, v, weight='BandwidthCost')

    #     # Remove paths from v to u
    #     from_v = v
    #     while from_v != u:
    #         h, t = list(self.sessions_tree[s_idx].in_edges(from_v))[0]
    #         # print "Remove ", h, t
    #         self.sessions_tree[s_idx].remove_edge(h, t)
    #         from_v = h
    #         self.app.links[(h, t)].used_cap -= self.sessions[s_idx].bw  # Remove path usage

    #     # Add new paths to the tree
    #     for i, head_edge in enumerate(new_path[:-1]):
    #         tail_edge = new_path[i+1]
    #         self.sessions_tree[s_idx]\
    #             .add_edge(head_edge, tail_edge,
    #                       weight=self.graph[head_edge][tail_edge]['BandwidthCost'] * self.sessions[s_idx].bw)
    #         self.app.links[(head_edge, tail_edge)].used_cap += self.sessions[s_idx].bw  # Add path usage
    #         if len(self.sessions_tree[s_idx].edges(head_edge)) > 1 or head_edge in self.sessions[s_idx].dsts:
    #             self.session_branch_nodes[s_idx].add(head_edge)
    #             self.branch_state_node_sessions[head_edge].add(s_idx)

    #     # Remove branch to u if necessary
    #     if len(self.sessions_tree[s_idx].edges(u)) == 0:
    #         # Remove paths from v to u
    #         from_u = u
    #         while from_u not in set(self.sessions[s_idx].dsts) | set(self.session_branch_nodes[s_idx]):
    #             h, t = list(self.sessions_tree[s_idx].in_edges(from_u))[0]
    #             self.sessions_tree[s_idx].remove_edge(h, t)
    #             from_u = h
    #             self.app.links[(h, t)].used_cap -= self.sessions[s_idx].bw  # Remove path usage

    #     return True

    # # Cost of set A in T_i
    # def cost_t_a(self, t_i, a_set):
    #     set_v = set(self.sessions[t_i].dsts) | a_set
    #     total_cost = 0
    #     for v in set_v:
    #         while v != self.sessions[t_i].src:
    #             head_edge, tail_edge = list(
    #                 self.sessions_tree[t_i].in_edges(v))[0]
    #             total_cost += self.sessions_tree[t_i][head_edge][tail_edge]['weight']
    #             v = head_edge
    #             if head_edge in a_set:
    #                 break
    #     return total_cost / self.beta

    # def dynamic_programming(self, number, capacity, weight_cost):
    #     """
    #     Solve the knapsack problem by finding the most valuable
    #     subsequence of `weight_cost` subject that weighs no more than
    #     `capacity`.
    #     Top-down solution from:
    #     http://codereview.stackexchange.com/questions/20569/dynamic-programming-solution-to-knapsack-problem
    #     :param weight_cost: is a sequence of pairs (weight, cost)
    #     :param capacity: is a non-negative integer
    #     :return: a pair whose first element is the sum of costs in the best combination,
    #     and whose second element is the combination.
    #     """

    #     # Return the value of the most valuable subsequence of the first i
    #     # elements in items whose weights sum to no more than j.
    #     @lru_cache
    #     def bestvalue(i, j):
    #         if i == 0:
    #             return 0
    #         weight, cost = weight_cost[i - 1]
    #         if weight > j:
    #             return bestvalue(i - 1, j)
    #         else:
    #             # maximizing the cost
    #             return max(bestvalue(i - 1, j), bestvalue(i - 1, j - weight) + cost)

    #     j = capacity
    #     result = [0] * number
    #     for i in xrange(len(weight_cost), 0, -1):
    #         if bestvalue(i, j) != bestvalue(i - 1, j):
    #             result[i - 1] = 1
    #             j -= weight_cost[i - 1][0]
    #     return bestvalue(len(weight_cost), capacity), result

    def optimize(self):

        start_time = time.time()

        ############################
        # Multi-Tree Routing Phase
        ############################

        copy_graph = self.graph.copy()

        # Sort the multicast tress according to their data rates in ascending order
        self.sessions = sorted(self.app.get_sessions(),
                               key=lambda session: session.bw)

        for s_idx, s_session in enumerate(self.sessions):
            for link_id in set(self.app.links.items()) & set(copy_graph.edges()):
                if self.links[link_id].cap - (self.links[link_id].used_cap + s_session.bw) < 0:
                    copy_graph.remove_edge(link_id[0], link_id[1])

            # All to All node shortest distance
            apsp = nx.johnson(copy_graph, weight='BandwidthCost')

            # Remove session which cannot reach to every destination
            if set(s_session.dsts) - set(apsp[s_session.src]):
                continue

            # Shortest path trees
            session_tree = nx.DiGraph()  # DAG - multicast tree
            counted_link = set()
            for dst in s_session.dsts:
                for i, head_edge in enumerate(apsp[s_session.src][dst][:-1]):  # path
                    tail_edge = apsp[s_session.src][dst][i + 1]
                    session_tree.add_edge(head_edge, tail_edge,
                                          weight=self.graph[head_edge][tail_edge]['BandwidthCost'] * s_session.bw)
                    # Add link usage
                    if (head_edge, tail_edge) not in counted_link:
                        self.links[(head_edge, tail_edge)].used_cap += s_session.bw
                        counted_link.add((head_edge, tail_edge))

                    # # Add flow table
                    self.node_capacity[head_edge] |= {s_idx}
                    self.node_capacity[tail_edge] |= {s_idx}

                    # Find branching nodes
                    if len(session_tree.edges(head_edge)) > 1:
                        self.session_branch_nodes[s_idx].add(head_edge)
            self.sessions_tree[s_idx] = session_tree

        # Try to reroute all branch node
        for s_idx, nodes in self.session_branch_nodes.items():
            for node in list(nodes):
                self.reroute(s_idx, node)
             
                       
             # self.reassess_tcam_cost()

        # max_tcam = 0
        # for node, sessions in self.node_capacity.items():
        #     if len(sessions)>max_tcam:
        #         max_tcam = len(sessions)

        total_c = 0
        for link_id, link in self.links.items():
            total_c += link.used_cap

        # print "max_tcam", max_tcam
        print "self.sessions_tree", len(self.sessions_tree)
        print "self.app.get_sessions()", len(self.app.get_sessions())

        print 'All DONE:', len(self.app.get_sessions()) - len(self.sessions_tree) == 0



        # ############################
        # # State-Node Assignment Phase
        # ############################
        #
        # print "State-Node Assignment Phase"
        #
        # # Greedy Assigning Stage
        # self.branch_state_node_sessions = defaultdict(set)  # Reset sessions_branch_state_nodes
        # self.session_branch_state_nodes = defaultdict(set)
        # a = defaultdict(set)
        # curr__a_cost = {s_idx: self.cost_t_a(s_idx, set()) for s_idx, _ in self.sessions_tree.items()}
        # cost_t_a_cache = dict()
        # for s_idx, _ in self.sessions_tree.items():
        #     cost_t_a_cache[s_idx] = defaultdict(float)
        # # Find max x
        # while True:
        #     max_x_cost, max_x_node, max_x_s_idx = -float('inf'), None, None
        #     for s_idx, _ in self.sessions_tree.items():
        #         for branch_node in self.session_branch_nodes[s_idx] - a[s_idx]:
        #             if len(self.branch_state_node_sessions[branch_node]) + 1 <= self.nodes[branch_node].services['sdn_router'].resources_cap['tcam']:
        #                 if tuple({branch_node} | a[s_idx]) not in cost_t_a_cache[s_idx]:
        #                     cost_t_a_cache[s_idx][tuple({branch_node} | a[s_idx])] = \
        #                         self.cost_t_a(s_idx, {branch_node} | a[s_idx])
        #                 reduced_cost = curr__a_cost[s_idx] - cost_t_a_cache[s_idx][tuple({branch_node} | a[s_idx])]
        #                 if reduced_cost > max_x_cost:
        #                     max_x_cost, max_x_node, max_x_s_idx = reduced_cost, branch_node, s_idx
        #
        #     if max_x_cost == -float('inf'):
        #         break
        #     a[max_x_s_idx].add(max_x_node)  # AU{x}
        #     curr__a_cost[max_x_s_idx] -= max_x_cost  # z(A)
        #     self.branch_state_node_sessions[max_x_node].add(max_x_s_idx)
        #     self.session_branch_state_nodes[max_x_s_idx].add(max_x_node)
        #
        # self.reassess_cost()
        #
        # total_c = 0
        # for link_id, link in self.links.items():
        #     total_c += link.used_cap
        #
        # print "total_c", total_c
        #
        #
        # # Local Search Stage
        #
        # print "Local Search Stage"
        #
        # # Identify the overloaded nodes
        # overloaded_branch_state_nodes = defaultdict(set)
        # for s_idx, branch_nodes in self.session_branch_nodes.items():
        #     for branch_node in branch_nodes:
        #         overloaded_branch_state_nodes[branch_node].add(s_idx)
        #         if len(overloaded_branch_state_nodes[branch_node]) > \
        #                 self.nodes[branch_node].services['sdn_router'].resources_cap['tcam']:
        #             self.overloaded_set.add(branch_node)
        #
        # # Choose set of session in branch state node that reduce z_cost
        # for u in self.overloaded_set:
        #     cost_no_overloaded_node = [(self.cost_t_a(s_idx, a[s_idx] - {u}) - self.cost_t_a(s_idx, a[s_idx] | {u}),
        #                                 s_idx) for s_idx in overloaded_branch_state_nodes[u]]
        #     cost_no_overloaded_node.sort()
        #
        #     # Remove overloaded node from branch state
        #     self.branch_state_node_sessions[u] = set()
        #     for s_idx in self.session_branch_state_nodes:
        #         if u in self.session_branch_state_nodes[s_idx]:
        #             self.session_branch_state_nodes[s_idx].remove(u)
        #
        #     for _ in range(self.nodes[u].services['sdn_router'].resources_cap['tcam']):
        #         _, best_t = cost_no_overloaded_node.pop()
        #         self.branch_state_node_sessions[u].add(best_t)
        #         self.session_branch_state_nodes[best_t].add(u)
        #         self.reassess_cost()
        #
        #     for _, s_idx in cost_no_overloaded_node:
        #         # Reroute the rest tree
        #         self.reroute(s_idx, u)
        #
        # # If the amount of multicast flows in any edge exceeds the capacity constraint,
        # # we also reroute its closest upstream state node u in a tree Ti
        # # to w, such that the new path from w to v follows the link
        # # capacity constraint.
        # overloaded_links = {link_id for link_id, link in self.links.items() if link.used_cap > link.cap}
        # for s_idx, s_session in self.sessions_tree.items():
        #     for h, t in list(overloaded_links):
        #         if (h, t) in s_session.edges():
        #             # find downstream
        #             down_streams = [t]
        #             while len(down_streams) > 0:
        #                 v = down_streams.pop(0)
        #                 if v in set(self.session_branch_nodes[s_idx]) | set(self.sessions[s_idx].dsts):
        #                     # find downstream
        #                     if self.reroute_path(s_idx, h, v):
        #                         overloaded_links.remove((h, t))
        #                         break
        #                 else:
        #                     down_streams += list(s_session.neighbors(v))
        #
        # # Remove session until link constraint is meant
        # while len(overloaded_links) > 0:
        #     overloaded_link = overloaded_links.pop()
        #     for s_idx in reversed(list(self.sessions_tree)):
        #         if overloaded_link in self.sessions_tree[s_idx].edges():
        #             del self.sessions_tree[s_idx]
        #             self.reassess_cost()
        #             if self.links[overloaded_link].used_cap <= self.links[overloaded_link].cap:
        #                 break


        # total_c = 0
        # for link_id, link in self.links.items():
        #     total_c += link.used_cap
        #
        # print "total_c", total_c
        # print "total mtrsa session", len(self.sessions_tree)
        # print "total session", len(self.app.get_sessions())
        # print "unicast tunneling", self.max_count_unicast_tunneling()
        #
        # for id, node in self.nodes.items():
        #     print "used cam", id, node.services['sdn_router'].used_cap['tcam']

        # Convert networkx to Solution
        for s_idx, s_session in self.sessions_tree.items():
            self._solution.get_tree(self.sessions[s_idx])
            self.update_session_cost(solution=self._solution, session=self.sessions[s_idx],  links=self.sessions_tree[s_idx].edges())

        total_time = time.time() - start_time
        self._solution.total_time = total_time
        return self._solution
