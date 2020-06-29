from ..base import Solver
from ..solution import Solution
import time
import networkx as nx
from collections import defaultdict
from msa_utils import steiner_tree
from ...multicast.session import Session
import statistics 

class ResourceOverloaded(Exception):
    pass

class MSASolver(Solver):



    def __init__(self, app, **kwargs):
        Solver.__init__(self, name='msa', app=app, **kwargs)

        self._solution = Solution()

        self.sessions = self.app.get_sessions()

        self.graph = self.app.graph
        self.nodes = self.app.nodes
        self.links = self.app.links

        self.service_chains = {s_idx: s_session.required_services for s_idx, s_session in enumerate(self.sessions)}
        self.service_nodes = defaultdict(set)
        for node in self.nodes:
            for key in self.nodes[node].services.keys():
                self.service_nodes[key].add(node)

        self.total_routing_cost = 0.0
        self.session_completed = 0.0

        self.solution_tree = defaultdict()


    def optimize(self):

        start_time = time.time()

        try:
            for s_idx, s_session in enumerate(self.sessions):
                session_start_time = time.time()
                tree = self._solution.get_tree(s_session)
                print "curr", s_idx
                ####################################################################################
                #
                # Algorithm 1 The construction of multilevel overlay directed network
                #
                ####################################################################################

                copy_graph = self.graph.copy()

                # Remove edge with insufficient residual capacity
                for link_id in set(self.app.links.items()) & set(copy_graph.edges()):
                    if self.links[link_id].cap - (self.links[link_id].used_cap + self.sessions[s_idx].bw) < 0:
                        copy_graph.remove_edge(link_id[0], link_id[1])

                # Calculate all shortest paths between each pair nodes in G
                apsp_length = dict(nx.all_pairs_dijkstra_path_length(copy_graph, weight='BandwidthCost'))
                apsp_path = nx.johnson(copy_graph, weight='BandwidthCost')

                

                # check if dst is reachable
                # Remove session which cannot reach to every destination
                try:
                    if set(self.sessions[s_idx].dsts) - set(apsp_path[self.sessions[s_idx].src]):
                        raise ResourceOverloaded
                except ResourceOverloaded:
                    print "ResourceOverloaded!!!!!!!!!!!!!!"
                    continue

                # Replicate all |V|=n nodes k times and place n*k nodes in a n x k grid, Each node can be denoted by v_n_k
                # Connect all nodes in i-th column to all nodes in i+1-th column with directed arrows
                # Set the edge cost between two nodes as the corresponding shortest path cost in G
                multilevel_graph = nx.DiGraph()
                for i, service in enumerate(self.service_chains[s_idx][:-1]):
                    for head in self.service_nodes[service]:
                        for tail in self.service_nodes[self.service_chains[s_idx][i + 1]]:
                            cost = apsp_length[head][tail]
                            multilevel_graph.add_edge(str(service) + "_" + str(head),
                                                    str(self.service_chains[s_idx][i + 1]) + "_" + str(tail), weight=cost)

                ####################################################################################
                #
                # Algorithm 2 The Modified Shortest-Path Algorithm (MSA)
                #
                ####################################################################################

                if len(self.service_chains[s_idx]) == 0:
                    st = steiner_tree(copy_graph, [self.sessions[s_idx].src] + list(self.sessions[s_idx].dsts), weight='BandwidthCost')
                    # Convert steiner_tree into directed graph
                    s_tree = nx.DiGraph()

                    for dst in self.sessions[s_idx].dsts:
                        s_tree.add_edge(dst, "sink", weight=0)
                    for h, t in st:
                        s_tree.add_edge(t, h, weight=copy_graph[t][h]['BandwidthCost'] * self.sessions[s_idx].bw)

                    st_edges = set()
                    for path in nx.all_simple_paths(s_tree.to_undirected(), source=self.sessions[s_idx].src, target="sink"):
                        for idx in range(len(path)-2):
                            st_edges.add((path[idx], path[idx+1]))
                        self._solution.add_path(self.sessions[s_idx], path[:-1], None)

                    memo = defaultdict(float)
                    # Update link usage
                    try:
                        for h, t in st_edges:
                            self.links[(h, t)].used_cap += self.sessions[s_idx].bw * self.links[(h, t)].bw_cost
                            memo[(h, t)] += self.sessions[s_idx].bw * self.links[(h, t)].bw_cost
                            self.total_routing_cost += self.sessions[s_idx].bw * self.links[(h, t)].bw_cost
                            if self.links[(h, t)].used_cap > self.links[(h, t)].cap:
                                raise ResourceOverloaded
                    except ResourceOverloaded:
                        print "ResourceOverloaded!!!!!!!!!!!!!!" 
                        for link_id, v in memo.items():
                            self.links[link_id].used_cap -= v
                        continue   
                    
                    curr_time = time.time() - session_start_time
                    self.update_session_cost(solution=self._solution, session=self.sessions[s_idx],  links=st_edges, time=curr_time)

                    self.solution_tree[s_idx] = s_tree

                    self.session_completed += 1
                    continue

                # Divide each nodes in G' into two parts, and connect theses two parts with cost y_l_k_u
                for service in self.service_chains[s_idx]:
                    for node in self.service_nodes[service]:
                        fst_half, snd_half = str(service) + "_" + str(node), '_' + str(service) + "_" + str(node)
                        for h, t in list(multilevel_graph.out_edges(fst_half)):
                            multilevel_graph.add_edge(snd_half, t, weight=multilevel_graph[h][t]['weight'])
                            multilevel_graph.remove_edge(h, t)
                        multilevel_graph.add_edge(fst_half, snd_half, weight=self.sessions[s_idx].res[service]['cpu'])

                # Add the source node S into G'
                # Connect S to all nodes of the first column in G' and set the cost as corresponding shortest path
                first_service = self.service_chains[s_idx][0]
                for tail in self.service_nodes[first_service]:
                    multilevel_graph.add_edge(self.sessions[s_idx].src,
                                            str(first_service) + "_" + str(tail),
                                            weight=apsp_length[self.sessions[s_idx].src][tail])

                feasible_solution = (float('inf'), [], [])
                last_service = self.service_chains[s_idx][-1]
                for v in self.service_nodes[last_service]:
                    if self.nodes[v].services[last_service].resources_cap['cpu'] - \
                            (self.nodes[v].services[last_service].used_cap['cpu'] +
                                self.sessions[s_idx].res[last_service]['cpu']) < 0:
                        continue

                    # Find the shortest path y from S to v in G' and get [w_l_n_u]
                    last_node_v = "_" + str(last_service) + "_" + str(v)

                    # Build a Steiner tree to cover v and all destinations
                    st = steiner_tree(copy_graph, [v] + list(self.sessions[s_idx].dsts), weight='BandwidthCost')
                    # Convert steiner_tree into directed graph
                    s_tree = nx.DiGraph()
                    s_tree_path = set()
                    for dst in self.sessions[s_idx].dsts:
                        s_tree.add_edge(dst, "sink", weight=0)
                    for h, t in st:

                        s_tree.add_edge(h, t, weight=copy_graph[h][t]['BandwidthCost'] * self.sessions[s_idx].bw)
                        s_tree.add_edge(t, h, weight=copy_graph[t][h]['BandwidthCost'] * self.sessions[s_idx].bw)
                    for path in nx.all_simple_paths(s_tree, source=v, target="sink"):
                        for i, n in enumerate(path[:-2]):
                            s_tree_path.add((n, path[i + 1]))

                    # steiner_tree cost
                    accum_cost = 0
                    for h, t in s_tree_path:
                        accum_cost += copy_graph[h][t]['BandwidthCost'] * self.sessions[s_idx].bw

                    shortest_path = nx.dijkstra_path(multilevel_graph,
                                                    self.sessions[s_idx].src, last_node_v, weight='weight')[1:]
                    candidate_solution = [self.sessions[s_idx].src]
                    for i, service in enumerate(self.service_chains[s_idx][:-1]):
                        # Check where l_j is deployed in an overloaded node, if so,
                        # find a new node with the minimum sum of setup cost and connection cost to deploy l_j
                        node = int(shortest_path[i * 2][len(str(service)) + 1:])
                        if self.nodes[node].services[service].resources_cap['cpu'] - \
                                (self.nodes[node].services[service].used_cap['cpu'] +
                                    self.sessions[s_idx].res[service]['cpu']) < 0:

                            other_nodes_cost = []
                            for other_node in set(self.service_nodes[service]) - {node}:
                                if self.nodes[other_node].services[service].resources_cap['cpu'] - \
                                        (self.nodes[other_node].services[service].used_cap['cpu'] +
                                            self.sessions[s_idx].res[service]['cpu']) < 0:
                                    continue

                                prev_service = "_" + self.service_chains[s_idx][i - 1] + "_" + str(candidate_solution[-1]) \
                                    if i != 0 else candidate_solution[-1]
                                next_service = self.service_chains[s_idx][i + 1] + "_" + shortest_path[(i + 1) * 2][
                                                                                        len(str(service)) + 1:]
                                vk = multilevel_graph[prev_service][str(service) + "_" + str(other_node)]['weight']
                                vm = multilevel_graph["_" + str(service) + "_" + str(other_node)][next_service]['weight']

                                other_nodes_cost.append((self.sessions[s_idx].res[service]['cpu'] + vk + vm, other_node))

                            if not other_nodes_cost:
                                # print ">>>>>Cannot find solution!solution!!!!"
                                break

                            _, best_candidate = max(other_nodes_cost)
                            candidate_solution.append(best_candidate)

                        else:
                            candidate_solution.append(node)
                    candidate_solution.append(v)  # add last node

                    # Cannot find solution with v!!!!!
                    if len(candidate_solution) < 1 + len(self.service_chains[s_idx]):
                        continue

                    # source to last VNF cost
                    for i, node in enumerate(candidate_solution[:-1]):
                        accum_cost += apsp_length[node][candidate_solution[i + 1]]

                    if accum_cost < feasible_solution[0]:
                        feasible_solution = (accum_cost, candidate_solution, s_tree_path)

                # Cannot find solution!!!!!
                try:
                    if len(feasible_solution[1]) < 1 + len(self.service_chains[s_idx]):
                        raise ResourceOverloaded
                except ResourceOverloaded:
                    print "ResourceOverloaded!!!!!!!!!!!!!!"
                    continue  

                traversed_edges = defaultdict(int)
                # Build tree from feasible solution
                feasible_solution_tree = nx.DiGraph()
                for node_id, node in enumerate(feasible_solution[1][:-1]):
                    path = nx.shortest_path(copy_graph, source=node, target=feasible_solution[1][node_id + 1],
                                            weight='BandwidthCost')
                    for i, h in enumerate(path[:-1]):
                        traversed_edges[(h, path[i + 1])] += 1
                        
                        feasible_solution_tree.add_edge(h, path[i + 1],
                                                        weight=copy_graph[h][path[i + 1]]['BandwidthCost'] *
                                                        self.sessions[s_idx].bw)
                for h, t in feasible_solution[2]:
                    traversed_edges[(h, t)] += 1
                    feasible_solution_tree.add_edge(h, t,
                                                    weight=copy_graph[h][t]['BandwidthCost'] * self.sessions[s_idx].bw)

                ####################################################################################
                #
                # Algorithm 3 The Optimize Phase Algorithm (OPA)
                #
                ####################################################################################

                _, candidate_solution, s_tree_path = feasible_solution

                # print "self.sessions[s_idx]", self.sessions[s_idx]

                curr_service_node = []
                for s_id, service_node in enumerate(candidate_solution[1:]):
                    curr_service_node.append((self.service_chains[s_idx][s_id], service_node))

                # Find all connection nodes in X_0
                connection_nodes = set(self.sessions[s_idx].dsts)
                for dst in self.sessions[s_idx].dsts:
                    connection_nodes -= set(nx.descendants(feasible_solution_tree, dst))

                for j, service in reversed(list(enumerate(self.service_chains[s_idx]))):
                    for connection_node in list(connection_nodes):
                        connection_nodes.remove(connection_node)
                        for other_node in set(self.service_nodes[service]) - {connection_node}:
                            # if l_j can be deployed in other nodes according to the rule in Section IV-C then
                            if self.nodes[other_node].services[service].resources_cap['cpu'] - \
                                    (self.nodes[other_node].services[service].used_cap['cpu'] +
                                        self.sessions[s_idx].res[service]['cpu']) < 0:
                                continue

                            old_cost = apsp_length[candidate_solution[j + 1]][connection_node]
                            new_cost = apsp_length[candidate_solution[j]][other_node] + \
                                    apsp_length[other_node][connection_node] + 0

                            if new_cost < old_cost:
                                curr_service_node.append((service, other_node))
                                # Old link from VNF to connection node
                                old_path = apsp_path[candidate_solution[j + 1]][connection_node]
                                for i, t in reversed(list(enumerate(old_path[1:]))):
                                    if feasible_solution_tree.has_edge(old_path[i], t) and feasible_solution_tree.edges(
                                            old_path[i]) <= 1 and feasible_solution_tree.edges(t) <= 1:
                                        feasible_solution_tree.remove_edge(old_path[i], t)
                                        traversed_edges[(old_path[i], t)] -= 1
                                    if feasible_solution_tree.has_node(old_path[i]) and len(feasible_solution_tree.edges(old_path[i])) > 1:
                                        break

                                # New link from new VNF to connection node
                                new_path = apsp_path[other_node][connection_node]
                                for i, h in enumerate(new_path[:-1]):
                                    try:
                                        feasible_solution_tree.add_edge(h, new_path[i + 1],
                                                                        weight=copy_graph[h][new_path[i + 1]]['BandwidthCost'] *
                                                                        self.sessions[s_idx].bw)
                                    except:
                                        print "i, h", i, h
                                        for hh, tt in copy_graph.edges():
                                            print hh, tt
                                    traversed_edges[(h, new_path[i + 1])] += 1

                                # New link from previous VNF to new VNF
                                new_path = apsp_path[candidate_solution[j]][other_node]
                                for i, h in enumerate(new_path[:-1]):
                                    feasible_solution_tree.add_edge(h, new_path[i + 1],
                                                                    weight=copy_graph[h][new_path[i + 1]]['BandwidthCost'] *
                                                                    self.sessions[s_idx].bw)
                                    traversed_edges[(h, new_path[i + 1])] += 1

                                connection_nodes.add(other_node)
                                break

                # Add link from source to connection nodes
                for connection_node in connection_nodes:
                    new_path = apsp_path[self.sessions[s_idx].src][connection_node]
                    for i, h in enumerate(new_path[:-1]):
                        feasible_solution_tree.add_edge(h, new_path[i + 1],
                                                        weight=copy_graph[h][new_path[i + 1]]['BandwidthCost'] *
                                                        self.sessions[s_idx].bw)
                        traversed_edges[(h, new_path[i + 1])] += 1

                print "self.sessions[s_idx]", self.sessions[s_idx].addr

                memo = defaultdict(float)
                try:
                    # Update service used_cap
                    for service, node in curr_service_node:
                        print "service, node", service, node
                        self.nodes[node].services[service].used_cap['cpu'] += self.sessions[s_idx].res[service]['cpu']
                        memo[service] += self.sessions[s_idx].res[service]['cpu']
                        if self.nodes[node].services[service].used_cap['cpu'] > self.nodes[node].services[service].resources_cap['cpu']:
                            raise ResourceOverloaded
                except ResourceOverloaded:
                    print "ResourceOverloaded!!!!!!!!!!!!!!" 
                    for service, v in memo.items():
                        node = curr_service_node[service]
                        self.nodes[node].services[service].used_cap['cpu'] -= v
                    continue   

                memo = defaultdict(float)
                # Update link usage
                try:
                    print traversed_edges.items()
                    curr_cost = 0
                    sess_links = []
                    for l, count_link in traversed_edges.items():
                        for _ in range(count_link):
                            h, t = l
                            print h, t
                            sess_links.append((h, t))
                            self.links[(h, t)].used_cap += self.sessions[s_idx].bw * self.links[(h, t)].bw_cost
                            memo[(h, t)] += self.sessions[s_idx].bw * self.links[(h, t)].bw_cost
                            if self.links[(h, t)].used_cap > self.links[(h, t)].cap:
                                raise ResourceOverloaded
                            curr_cost += self.sessions[s_idx].bw * self.links[(h, t)].bw_cost
                            self.total_routing_cost += self.sessions[s_idx].bw * self.links[(h, t)].bw_cost
                    print self.sessions[s_idx].addr, "curr_cost", curr_cost
                except ResourceOverloaded:
                    print "ResourceOverloaded!!!!!!!!!!!!!!" 
                    for link_id, v in memo.items():
                        self.links[link_id].used_cap -= v
                    continue   

                # # Update the flow tcam
                # for node, num_traverse in traversed_edges.items():
                #     h, t = node
                #     self.nodes[h].services['sdn_router'].used_cap['tcam'] = self.sessions[s_idx].res[service]['tcam'] * num_traverse
                #     self.nodes[t].services['sdn_router'].used_cap['tcam'] = self.sessions[s_idx].res[service]['tcam'] * num_traverse

                # num_undirected_traversed_edges = defaultdict(int)
                # for h, t in traversed_edges.keys():
                #     # h, t = key
                #     if (h, t) in num_undirected_traversed_edges or (t, h) in num_undirected_traversed_edges:
                #         continue
                #     num_undirected_traversed_edges[(h, t)] = traversed_edges[(h, t)] + traversed_edges[(t, h)]

                curr_time = time.time() - session_start_time
                self.update_session_cost(solution=self._solution, session=self.sessions[s_idx],  links=sess_links, time=curr_time)

                self.session_completed += 1


     

        except Exception as e:
            raise e
        
        finally:
            print 'All DONE:', str(self.session_completed/len(self.sessions))
            total_time = time.time() - start_time
            print "Optimization time       = ", str(total_time), " sec"
            self._solution.total_time = total_time
            return self._solution
