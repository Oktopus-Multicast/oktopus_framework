import sys
import time
import networkx as nx
from copy import deepcopy
from collections import OrderedDict, defaultdict

GRAPHVIZ = True

try:
    import graphviz
except ImportError as e:
    print e
    GRAPHVIZ = False


from base import Solver
from solution import Solution
from oktopus_utils import pick_dst, calculate_simple_paths_regenerate, calculate_simple_paths3, calculate_simple_paths, calculate_simple_paths_trie, calculate_simple_paths_shrink
from ..multicast.session import Session

MAX_PATH_COST = 10 ** 12

class SSPResult:
    def __init__(self, found=False, cost=MAX_PATH_COST, path=None, path_nodes=None, srv_map=None, optimized=False):
        self.found = found
        self.cost = cost
        self.path = path
        self.path_nodes = path_nodes
        self.srv_map = srv_map
        self.optimized = optimized


class _SFPath:
    def __init__(self, cost, path_nodes, path_links, srv_map, parent_sf_path):
        self.cost = cost
        self.path_nodes = path_nodes
        self.path_links = path_links
        self.srv_map = srv_map
        self.parent_sf_path = parent_sf_path
        self.inc_pkt_type = {}
        self.out_pkt_type = {}

        self._calculate_inc_pkt_type()
        self._calculate_out_pkt_type()

    def _calculate_inc_pkt_type(self):
        cur_inc_pkt_type = []
        first_node_id = self.path_nodes[0]
        if self.parent_sf_path:
            cur_inc_pkt_type = self.parent_sf_path.inc_pkt_type[first_node_id][:]

        self.inc_pkt_type[first_node_id] = cur_inc_pkt_type[:]

        # if self.parent_sf_path and first_node_id in self.parent_sf_path.srv_map:
        #     cur_inc_pkt_type.extend(self.parent_sf_path.srv_map[first_node_id])
        #
        if self.parent_sf_path:
            cur_inc_pkt_type = self.parent_sf_path.out_pkt_type[first_node_id][:]
        if first_node_id in self.srv_map:
            cur_inc_pkt_type.extend(self.srv_map[first_node_id])

        for node_id in self.path_nodes[1:]:
            self.inc_pkt_type[node_id] = cur_inc_pkt_type[:]
            if node_id in self.srv_map:
                cur_inc_pkt_type.extend(self.srv_map[node_id])

    def _calculate_out_pkt_type(self):
        cur_out_pkt_type = []
        first_node_id = self.path_nodes[0]
        if self.parent_sf_path:
            cur_out_pkt_type = self.parent_sf_path.out_pkt_type[first_node_id][:]

        # if self.parent_sf_path and first_node_id in self.parent_sf_path.srv_map:
        #     cur_out_pkt_type.extend(self.parent_sf_path.srv_map[first_node_id])

        if first_node_id in self.srv_map:
            cur_out_pkt_type.extend(self.srv_map[first_node_id])

        self.out_pkt_type[first_node_id] = cur_out_pkt_type[:]

        for node_id in self.path_nodes[1:]:
            if node_id in self.srv_map:
                cur_out_pkt_type.extend(self.srv_map[node_id])
            self.out_pkt_type[node_id] = cur_out_pkt_type[:]


class OktopusSolver(Solver):
    def __init__(self, app, **kwargs):
        Solver.__init__(self, name='oktopus', app=app, **kwargs)

        self._solution = Solution()
        # self._cutoff = 0
        self._ppp = 0
        self._pool_size = 4

        # oktopus data structures
        self._simple_paths = {}
        self._simple_paths_nodes = {}
        self._simple_paths_links_ids = {}
        self._links_map = {}
        self._link_load_constraint_set = set()
        self._node_tcam_constraint_set = set()
        self._path_link_load_constraint_map = {}
        self._path_node_tcam_load_constraint_map = {}
        # per session per node
        self._delay_map = {}
        self._invalid_delay_map = {}
        self._pass_map_src = {}

        # self._sp_draw_graph = True and GRAPHVIZ
        self._sp_draw_graph = False and GRAPHVIZ
        self._sp_graphviz = {}
        self._sp_pkt_type = {}
        self._sp_first_dst = {}
        self._sp_inc_pkt_type = {}
        self._sp_out_pkt_type = {}
        self._sp_has_path = {}
        self._sp_solution = {}
        self._sp_sf_solution = {}
        self._sp_joined_receivers = {}

        self.x1_total_time = 0

        # print len(self.app.graph.edges())
        # exit(0)

        self._init_oktopus()

    def _init_oktopus(self):
        nodes_count = len(self.app.get_nodes())
        # self._cutoff = int(self.options.pop('cutoff', nodes_count - 1))
        # if self._cutoff < 0 or self._cutoff > nodes_count:
        #     self._cutoff = int(nodes_count - 1)

        self._ppp = int(self.options.pop('ppp', 10000))
        self._pool_size = int(self.options.pop('pool_size', 4))
        if self._ppp < 0:
            self._ppp = 10000
        if self._pool_size < 0:
            self._pool_size = 4
        
        self._ppp = 10

        # print '[oktopus] using cutoff=', self._cutoff
        print '[oktopus] using ppp =', self._ppp
        print '[oktopus] using pool size =', self._pool_size

        if self.cache_dir:
            print '[oktopus] caching is not currently supported in oktopus solver :('

        links = self.app.links.values()
        self._links_map = {(l.src, l.dst): l for l in links}

        self._links_map_src_dst = {(l.src, l.dst): set() for l in links}
        self.overloaded_link = set()

        # Initialize internal structures related to resource constraints
        self._init_link_constraints_map()
        self._init_node_constraints_map()
        self._init_sf_structures()
        self._init_delay_maps()
        
        self.commun = nx.communicability_exp(self.app.graph.to_undirected())    
        self.commun = nx.all_pairs_dijkstra_path_length(self.app.graph.to_undirected())

        betweenness = nx.edge_betweenness_centrality(self.app.graph)
        for k, l in self._links_map.items():
            l.betweeness_score = betweenness[k]

        # simple_paths_dict = calculate_simple_paths_trie(self.app.graph, self._links_map, ppp=self._ppp,
        #                                            pool_size=self._pool_size)
        # for path_key, path_trie in simple_paths_dict.iteritems():
        #     if path_key == (1, 12):
        #         trie = path_trie
        #         trie.traverse()

        # Path Generation
        simple_paths_dict = calculate_simple_paths(self.app.graph, self._links_map, ppp=self._ppp,
                                                   pool_size=self._pool_size)
        # simple_paths_dict = calculate_simple_paths_shrink(self.app.graph, self._links_map, ppp=self._ppp,
        #                                             pool_size=self._pool_size)
        for path_key, path_tuples in simple_paths_dict.iteritems():
            # population_size = len(path_tuples[0])
            # max_population = int (population_size / 2)
            # path_tuples[0]: list of link objects
            # path_tuples[1]: list of list of node ids
            # path_tuples[2]: list of list of (link.src, link.dst) tuples

            # print path_key, len(path_tuples[0])

            self._simple_paths[path_key] = path_tuples[0]
            self._simple_paths_nodes[path_key] = path_tuples[1]
            self._simple_paths_links_ids[path_key] = path_tuples[2]
            
            for p in self._simple_paths_links_ids[path_key]:
                for link_id in p:
                    self._links_map_src_dst[link_id].add((path_key))

            for path_idx, path_link_ids in enumerate(path_tuples[2]):
                constrained_link_ids = set(path_link_ids).intersection(self._link_load_constraint_set)
                if constrained_link_ids:
                    if path_key not in self._path_link_load_constraint_map:
                        self._path_link_load_constraint_map[path_key] = {}
                    if path_idx not in self._path_link_load_constraint_map[path_key]:
                        self._path_link_load_constraint_map[path_key][path_idx] = constrained_link_ids

            for path_idx, path_node_ids in enumerate(path_tuples[1]):
                constrained_node_ids = set(path_node_ids).intersection(self._node_tcam_constraint_set)
                if constrained_node_ids:
                    if path_key not in self._path_node_tcam_load_constraint_map:
                        self._path_node_tcam_load_constraint_map[path_key] = {}
                    if path_idx not in self._path_node_tcam_load_constraint_map[path_key]:
                        self._path_node_tcam_load_constraint_map[path_key][path_idx] = constrained_node_ids
        print '[oktopus] initialization done'
        # exit(0)

    def _init_sf_structures(self):
        for s in self.app.get_sessions():
            if s.required_services:
                self._sp_pkt_type[s.addr] = {}
                if self._sp_draw_graph:
                    self._sp_graphviz[s.addr] = graphviz.Digraph(s.addr, filename='%s.gv' % s.addr)
                self._sp_inc_pkt_type[s.addr] = defaultdict(list)
                self._sp_out_pkt_type[s.addr] = defaultdict(list)
                self._sp_first_dst[s.addr] = None
                for n in self.app.get_nodes():
                    self._sp_pkt_type[s.addr][n.node_id] = []
                    # self._sp_inc_pkt_type[s.addr][n.node_id] = []
                    # self._sp_out_pkt_type[s.addr][n.node_id] = []
                self._sp_has_path[s.addr] = False
                self._sp_solution[s.addr] = []
                self._sp_sf_solution[s.addr] = []
                self._sp_joined_receivers[s.addr] = set()

    def _init_link_constraints_map(self):
        links = self.app.links.values()
        for link in links:
            load_constraint = link.constraint_map.get('load', -1)
            if load_constraint > 0:
                self._link_load_constraint_set.add((link.src, link.dst))

    def _init_node_constraints_map(self):
        nodes = self.app.nodes.values()
        for node in nodes:
            node_constraint = -1
            node_dict = node.constraint_map.get('sdn_router', -1)
            if type(node_dict) is dict:
                node_constraint = node_dict.get('tcam', -1)
            if node_constraint > 0:
                self._node_tcam_constraint_set.add(node.node_id)

    def _init_delay_maps(self):
        for session in self.app.get_sessions():
            self._delay_map[session.addr] = {}
            # cumulative link delay at src is zero
            self._delay_map[session.addr][session.src] = 0

        for session in self.app.get_sessions():
            max_delay = session.constraint_map['delay']

            if max_delay > 0:
                self._invalid_delay_map[session.addr] = []
                for dst in session.dsts:
                    invalid_delay_count = 0
                    sps = self._simple_paths[session.src, dst]
                    for simple_path in sps:
                        delay = 0
                        for link in simple_path:
                            delay += link.delay
                        if delay > max_delay:
                            invalid_delay_count += 1
                    dst_delay_tuple = (invalid_delay_count / float(len(sps)), dst)
                    self._invalid_delay_map[session.addr].append(dst_delay_tuple)

        for key, val in self._invalid_delay_map.iteritems():
            if val:
                sorted_list = sorted(val, reverse=True)
                self._invalid_delay_map[key] = [x[1] for x in sorted_list]

    @classmethod
    def _sort_session_key(cls, session, max_bw):
        assert isinstance(session, Session)
        session_value = (session.bw / float(max_bw)) * len(session.dsts)
        if session.constraint_map['load'] > 0:
            session_value *= 5
        if session.constraint_map['delay'] > 0:
            session_value *= 5
        if session.constraint_map['hops'] > 0:
            session_value *= 5
        if session.pass_map['nodes']:
            session_value *= 4
        if session.required_services:
            session_value *= len(session.required_services)
        if session.avoid_map['nodes']:
            session_value *= 2
        if session.avoid_map['links']:
            session_value *= 2
        if session.avoid_map['sessions']:
            session_value *= 2
        return session_value

    def _sort_sessions(self):
        sessions = self.app.get_sessions()
        max_bw = max(s.bw for s in sessions)
        sorted_sessions = sorted(sessions, key=lambda s: self._sort_session_key(s, max_bw), reverse=True)
        return sorted_sessions

    def _calculate_node_cost(self, session, node, service):
        path_cost = 0
        # if self.app.routing.obj_min_mnu:
        fn_dict = self.app.routing.node_cost_fn_map.get(node.node_id)
        if fn_dict:
            srv_name = service.name
            required_res_dict = session.res[srv_name]
            for res_name, res_value in required_res_dict.iteritems():
                if res_name in service.resources_cap:
                    # print srv_name, res_name
                    fn = fn_dict[srv_name][res_name]
                    path_cost += fn(node, session, srv_name, res_name)
                else:
                    print 'node cost: service has no resource'
                    path_cost += MAX_PATH_COST
            return path_cost
        # else:
        #     print 'node cost: no fn_dict'
        return MAX_PATH_COST

    def _calculate_links_costs(self, session, links):
        final_cost = 1
        max_load = 0
        total_delay = 0
        routing_cost = 0
        for link in links:
            if link:
                fn = self.app.routing.link_cost_fn_map.get(link.link_id)
                if fn:
                    link_load = fn(link, session)
                    if link_load > max_load:
                        max_load = link_load
                    routing_cost += link_load
                else:
                    print 'link cost: no fn'
                total_delay += link.delay
            else:
                raise ValueError('Not a valid link')
        
        if self.app.routing.obj_min_mlu:
            final_cost *= max_load
        if self.app.routing.obj_min_delay:
            final_cost *= total_delay
        if self.app.routing.obj_min_routing_cost:
            final_cost = routing_cost
        if not self.app.routing.obj_min_mlu and not self.app.routing.obj_min_delay and not self.app.routing.obj_min_routing_cost:
            final_cost = len(links)

        return final_cost

    def _calculate_nodes_costs(self, session, node_ids, srv_dict):
        reversed_srv_dir = {}
        for k, v in srv_dict.iteritems():
            for vv in v:
                reversed_srv_dir[vv] = k
        path_cost = 0
        req_services = set(session.res.keys())
        non_ordered_services = req_services.difference(set(reversed_srv_dir.keys()))

        # node cost
        for node_id in node_ids:
            node = self.app.get_node(node_id)
            if node:
                # ordered services
                if node_id in srv_dict:
                    srv_name_list = srv_dict[node_id]
                    for srv_name in srv_name_list:
                        service = node.get_service(srv_name)
                        path_cost += self._calculate_node_cost(session, node, service)
                for not_ordered_srv_name in non_ordered_services:
                    service = node.get_service(not_ordered_srv_name)
                    if service and not service.ordered:
                        path_cost += self._calculate_node_cost(session, node, service)
        return path_cost

    def _calculate_path_cost(self, session, node_ids, links, srv_dict):
        assert isinstance(session, Session)
        link_cost = self._calculate_links_costs(session, links)
        node_cost = self._calculate_nodes_costs(session, node_ids, srv_dict)
        if self.app.routing.obj_min_routing_cost:
            link_cost = link_cost #/ float(10000000000)
        # print 'link cost =', link_cost, 'node cost =', node_cost
        path_cost = link_cost #+ node_cost
        return path_cost

    def _is_pass_satisfied(self, path_nodes, pass_through_nodes, ignore_nodes=None):
        max_node_idx = self._calculate_max_srv_idx(path_nodes, ignore_nodes)
        _pass_idx = 0
        _node_idx = 0
        while _node_idx <= max_node_idx and _pass_idx < len(pass_through_nodes):
            pass_node_id = pass_through_nodes[_pass_idx]
            node_id = path_nodes[_node_idx]
            if node_id == pass_node_id:
                _pass_idx += 1
            _node_idx += 1
        return _pass_idx == len(pass_through_nodes)

    @classmethod
    def _is_complete_service_path(cls, req_services, srv_map):
        return len(req_services) == sum(len(v) for v in srv_map.values())

    def _generate_base_service_path(self, path, node_index, req_services, max_srv_idx):
        srv_map = OrderedDict()
        _srv_idx = 0
        _node_idx = node_index
        while _node_idx <= max_srv_idx and _srv_idx < len(req_services):
            srv_name = req_services[_srv_idx]
            node_ids = self.app.get_nodes_ids_by_service(srv_name)
            node_id = path[_node_idx]
            if node_id in node_ids:
                # if we find a service in this node, look at the next service
                if node_id not in srv_map:
                    srv_map[node_id] = []
                srv_map[node_id].append(srv_name)
                _srv_idx += 1
            else:
                # this to make sure that one node can support multiple services
                _node_idx += 1
        return srv_map

    def _generate_service_node_combination(self, path, req_services, srv_name, max_node_idx, srv_map, final_results):
        found_node_id = -1
        new_srv_map = deepcopy(srv_map)
        node_ids = self.app.get_nodes_ids_by_service(srv_name)
        srv_idx = req_services.index(srv_name)
        for node_id, srv_list in new_srv_map.iteritems():
            if srv_name in srv_list:
                found_node_id = node_id
        if found_node_id > -1:
            new_srv_map[found_node_id].remove(srv_name)
            if not new_srv_map[found_node_id]:
                del new_srv_map[found_node_id]

            for _node_idx in range(found_node_id, max_node_idx + 1):
                if path[_node_idx] in node_ids:
                    node_id = path[_node_idx]
                    srv_map_copy = deepcopy(new_srv_map)
                    if node_id not in srv_map_copy:
                        srv_map_copy[node_id] = []
                    srv_map_copy[node_id].append(srv_name)

                    # reorder services
                    final_srv_map = OrderedDict()
                    for _path_node in path:
                        if _path_node in srv_map_copy:
                            final_srv_map[_path_node] = srv_map_copy[_path_node]
                            reordered_services = []
                            for _srv in req_services:
                                if _srv in final_srv_map[_path_node]:
                                    reordered_services.append(_srv)
                            final_srv_map[_path_node] = reordered_services
                    final_results.append(final_srv_map)
                    next_max_node_idx = path.index(node_id)

                    if srv_idx > 0:
                        next_srv_name = req_services[srv_idx - 1]
                        self._generate_service_node_combination(path, req_services, next_srv_name, next_max_node_idx,
                                                                srv_map_copy, final_results)

    @classmethod
    def _calculate_max_srv_idx(cls, path_nodes, ignore_nodes):
        ignore_idx = len(path_nodes)
        if ignore_nodes:
            idx_list = [path_nodes.index(x) for x in ignore_nodes if x in path_nodes]
            if idx_list:
                ignore_idx = min(idx_list)

        return ignore_idx - 1

    def _filter_simple_service_paths(self, session, src, dst, services, ignore_nodes=None):
        all_dsts = session.dsts
        other_dsts = set(all_dsts).difference({dst, src})
        ignore_nodes_set = set()
        if ignore_nodes:
            ignore_nodes_set = set(ignore_nodes).difference({src})

        tree = self._solution.get_tree(session)
        min_cost = float("inf")
        min_cost_path = None
        min_cost_path_nodes = None
        min_cost_node_srv_map = None

        min_valid_cost = float("inf")
        min_cost_valid_path = None
        min_cost_valid_path_nodes = None
        min_cost_valid_node_srv_map = None

        # constrained_node_ids = set()
        # constrained_link_ids = set()
        #
        # for node_id in self._node_tcam_constraint_set:
        #     node = self.app.get_node(node_id)
        #     srv = node.get_service('sdn_router')
        #     if srv.get_available_cap('tcam') <= 0:
        #         constrained_node_ids.add(node_id)
        #
        # for link_id in self._link_load_constraint_set:
        #     c_link = self._links_map[link_id]
        #     c_link_max_load = c_link.constraint_map['load']
        #     new_load = (c_link.used_cap + session.bw) / float(c_link.cap)
        #     if new_load > c_link_max_load:
        #         constrained_link_ids.add(link_id)
        
        for path_idx, path in enumerate(self._simple_paths[src, dst]):
            srv_found = True
            node_srv_map = OrderedDict()
            path_nodes = self._simple_paths_nodes[src, dst][path_idx]
            path_links_ids_set = set(self._simple_paths_links_ids[src, dst][path_idx])
            all_path_nodes_set = set(path_nodes)

            # this to make sure that this path has no loops
            if all_path_nodes_set.difference(ignore_nodes_set) != all_path_nodes_set:
                continue

            # to support avoid(nodes) API
            if all_path_nodes_set.difference(session.avoid_map['nodes']) != all_path_nodes_set:
                continue

            # # to support hard capacity constraints for TCAM
            # if all_path_nodes_set.difference(constrained_node_ids) != all_path_nodes_set:
            #     continue

            # to support avoid(links) API
            if path_links_ids_set.difference(session.avoid_map['links']) != path_links_ids_set:
                continue

            # # to support hard capacity constraints for links
            # if path_links_ids_set.difference(constrained_link_ids) != path_links_ids_set:
            #     continue

            # to satisfy link load constraints
            path_key = (src, dst)
            loaded_link = False
            if path_key in self._path_link_load_constraint_map:
                if path_idx in self._path_link_load_constraint_map[path_key]:
                    constrained_link_ids = self._path_link_load_constraint_map[path_key][path_idx]
                    for c_link_id in constrained_link_ids:
                        c_link = self._links_map[c_link_id]
                        c_link_max_load = c_link.constraint_map['load']
                        new_load = (c_link.used_cap + session.bw) / float(c_link.cap)
                        if new_load > c_link_max_load:
                            loaded_link = True
                            print c_link_id, c_link.used_cap, session.bw
                            break

            if loaded_link:
                continue
            loaded_tcam_node = False
            if path_key in self._path_node_tcam_load_constraint_map:
                if path_idx in self._path_node_tcam_load_constraint_map[path_key]:
                    constrained_node_ids = self._path_node_tcam_load_constraint_map[path_key][path_idx]
                    for c_node_id in constrained_node_ids:
                        node = self.app.get_node(c_node_id)
                        srv = node.get_service('sdn_router')
                        if srv.get_available_cap('tcam') <= 0:
                            loaded_tcam_node = True
                            break
            if loaded_tcam_node:
                continue

            # to satisfy session.hops
            long_path = False
            if session.constraint_map['hops'] > 0:
                session_max_hops = session.constraint_map['hops']
                offset = tree.get_hop_count(path_nodes[0])
                hop_count = offset + len(path)
                if hop_count > session_max_hops:
                    long_path = True

            if long_path:
                continue

            # to support avoid(sessions) API
            if session.avoid_map['sessions']:
                avoid_nodes = set()
                avoid_links = set()
                for addr in session.avoid_map['sessions']:
                    other_session = self.app.get_session(addr)
                    if other_session and self._solution.is_session_done(other_session):
                        other_tree = self._solution.get_tree(other_session)
                        other_nodes_ids = other_tree.get_nodes_ids()
                        other_links_ids = other_tree.get_links_ids()
                        avoid_nodes = avoid_nodes.union(other_nodes_ids)
                        avoid_links = avoid_links.union(other_links_ids)
                if all_path_nodes_set.difference(avoid_nodes) != all_path_nodes_set:
                    continue
                if path_links_ids_set.difference(avoid_links) != path_links_ids_set:
                    continue

            # to support pass(nodes) APIs
            if session.pass_map['nodes'] and not tree.has_path:
                pass_through_satisfied = False
                pass_through_nodes = None
                # pass_ignore = set(session.dsts).union(ignore_nodes_set).difference(tree.joined_receivers)
                for pass_through in session.pass_map['nodes']:
                    if self._is_pass_satisfied(path_nodes, pass_through, ignore_nodes=other_dsts):
                        pass_through_satisfied = True
                        pass_through_nodes = pass_through
                        break
                if not pass_through_satisfied:
                    continue

                self._pass_map_src[session.addr] = pass_through_nodes[-1]

            # TODO compare session.delay before and after applying delay constraint
            # to support service chaining requirements
            if services:
                # this makes sure that no dst exists before any service (to ensure to service path loops)
                max_srv_idx = self._calculate_max_srv_idx(path_nodes, other_dsts)
                last_srv = services[-1]
                min_node_cost = float("inf")

                for node_idx, _ in enumerate(path_nodes):
                    node_srv_map_list = []
                    base_srv_map = self._generate_base_service_path(path_nodes, node_idx, services, max_srv_idx)
                    # if self._is_complete_service_path(services, base_srv_map):
                    #     print base_srv_map
                    if self._is_complete_service_path(services, base_srv_map):
                        if self.app.routing.obj_min_mnu:
                            node_srv_map_list.append(base_srv_map)
                            self._generate_service_node_combination(path_nodes, services, last_srv, max_srv_idx,
                                                                    base_srv_map, node_srv_map_list)
                            for srv_map in node_srv_map_list:
                                node_cost = self._calculate_nodes_costs(session, path_nodes, srv_map)
                                if node_cost < min_node_cost:
                                    min_node_cost = node_cost
                                    node_srv_map = srv_map
                        else:
                            node_srv_map = base_srv_map
                            break

                # make sure this map/dict includes the same number of services of the required ones
                # this doesn't check the order, since the previous operation does
                # the goal here is replace this path with a previous found path iff this new path:
                # (1) has the service requirements and (2) has less cost than the previous one
                if not self._is_complete_service_path(services, node_srv_map):
                    srv_found = False

            # to support session.delay < max_delay requirements
            # here, session.delay is not a strict constraint; this means that we allow a
            # session with delay > max_delay if it has no better paths
            session_delay_satisfied = True
            max_delay = session.constraint_map['delay']
            if max_delay > 0:
                for link in path:
                    self._delay_map[session.addr][link.dst] = link.delay + \
                                                              self._delay_map[session.addr][link.src]
                dst_delay = self._delay_map[session.addr][dst]
                if dst_delay > max_delay:
                    session_delay_satisfied = False

            constraints_satisfied = session_delay_satisfied

            # valid and min cost path
            path_cost = self._calculate_path_cost(session, path_nodes, path, node_srv_map)
            if srv_found and path_cost < min_cost:
                min_cost = path_cost
                min_cost_path = path
                min_cost_path_nodes = path_nodes
                min_cost_node_srv_map = node_srv_map

            if constraints_satisfied:
                if srv_found and path_cost < min_valid_cost:
                    min_valid_cost = path_cost
                    min_cost_valid_path = path
                    min_cost_valid_path_nodes = path_nodes
                    min_cost_valid_node_srv_map = node_srv_map

        if min_cost_valid_path:
            # self.x1_total_time += time.time() - tt
            return SSPResult(found=True, cost=min_valid_cost, path=min_cost_valid_path,
                             path_nodes=min_cost_valid_path_nodes, srv_map=min_cost_valid_node_srv_map, optimized=True)

        found = False
        optimized = False
        if min_cost_path_nodes:
            found = True
            optimized = False
        return SSPResult(found=found, cost=min_cost, path=min_cost_path, path_nodes=min_cost_path_nodes,
                         srv_map=min_cost_node_srv_map, optimized=optimized)

    def _match_sf_to_path(self, path, node_index, req_services):
        srv_map = OrderedDict()
        _srv_idx = 0
        _node_idx = node_index
        last_node_idx = node_index # last service node
        while _node_idx < len(path) and _srv_idx < len(req_services):
            srv_name = req_services[_srv_idx]
            node_ids = self.app.get_nodes_ids_by_service(srv_name)
            node_id = path[_node_idx]
            if node_id in node_ids:
                # if we find a service in this node, look at the next service
                if node_id not in srv_map:
                    srv_map[node_id] = []
                srv_map[node_id].append(srv_name)
                _srv_idx += 1
                last_node_idx = _node_idx
            else:
                # this to make sure that one node can support multiple services
                _node_idx += 1
        return _srv_idx, last_node_idx, srv_map

    def _get_max_common_path(self, path_nodes_1, path_nodes_2, srv_map_1, srv_map_2):
        common_path = []

        common_nodes = set(path_nodes_1).intersection(set(path_nodes_2))
        if not common_nodes or len(common_nodes) == 1:
            return common_path

        first_found = False

        i = min(path_nodes_1.index(c) for c in common_nodes)  # path_nodes_1.index(path_nodes_2[0])
        j = 0
        while i < len(path_nodes_1) and j < len(path_nodes_2):
            srv_1 = srv_map_1.get(path_nodes_1[i], None)
            srv_2 = srv_map_2.get(path_nodes_2[j], None)
            if path_nodes_1[i] == path_nodes_2[j] and srv_1 == srv_2:
                first_found = True
                common_path.append(path_nodes_1[i])
                i += 1
                j += 1
            else:
                i += 1
                if first_found:
                    break

        if len(common_path) > 1:
            return common_path
        return []

    def _update_sf_resources(self, session, path_links, path_nodes, srv_map, link_usage_fn=None):
        # print '>>>', session.addr
        # print '>>>>>>', [l.link_id for l in path_links]

        for sp_prev_sol in reversed(self._sp_solution[session.addr]):
            # Each item is a list of tuples representing a path to a prev destination
            # Each tuple has two parts: a path as a list of nodes, and service map as an OrderedDict
            for sp_tuple in reversed(sp_prev_sol):
                sp_path = sp_tuple[0]
                sp_srv_map = sp_tuple[1]
                common_path = self._get_max_common_path(sp_path, path_nodes, sp_srv_map, srv_map)
                if common_path:
                    print 'Common Path', sp_path, path_nodes, common_path

        for link in path_links:
            # link = self._links_map[link_id]
            print '(1)', link.link_id, link.used_cap
            fn = link_usage_fn if link_usage_fn else self.app.routing.link_usage_fn_map[link.link_id]
            fn(link, session)
            print '(2)', link.link_id, link.used_cap

    @classmethod
    def _get_max_services(cls, required_services, node_services):
        services = []
        for s in required_services:
            if s in node_services:
                services.append(s)
            else:
                break
        return services

    @classmethod
    def _get_diff_services(cls, required_services, max_services):
        diff_services = []
        for s in required_services:
            if s not in max_services:
                diff_services.append(s)
        return diff_services

    def _find_sf_path(self, session, src, dst, services, parent_pkt_type=None, final_paths=None, backtrace=None):
        found = False
        current_cost = float('inf')
        path_cost = float('inf')
        max_service_len = 0
        # A path with min cost and max number of required services
        service_path = None
        next_path = []
        next_services = []
        if src != dst:
            # link so far in final_paths
            final_paths_link_dict = defaultdict(int)
            for p in final_paths:
                for l in p[3]:
                    final_paths_link_dict[l.link_id] += 1

            copy_graph = nx.DiGraph()

            for link_id, _ in self.app.links.items():
                copy_graph.add_edge(link_id[0], link_id[1])

            # Remove edge with insufficient residual capacity
            for link_id, _ in self.app.links.items():
                if self._links_map[link_id].cap - (self._links_map[link_id].used_cap + session.bw ) < 0:
                    copy_graph.remove_edge(link_id[0], link_id[1])

                if link_id in final_paths_link_dict and  self._links_map[link_id].cap - (self._links_map[link_id].used_cap + session.bw + final_paths_link_dict[link_id]*session.bw ) < 0:
                    copy_graph.remove_edge(link_id[0], link_id[1])

            for path_idx, path in enumerate(self._simple_paths[src, dst]):
                # to satisfy link load constraints
                path_key = (src, dst)
                loaded_link = False
                if path_key in self._path_link_load_constraint_map:
                    if path_idx in self._path_link_load_constraint_map[path_key]:
                        constrained_link_ids = self._path_link_load_constraint_map[path_key][path_idx]
                        for c_link_id in constrained_link_ids:
                            c_link = self._links_map[c_link_id]
                            c_link_max_load = c_link.constraint_map['load']
                            sess_bw = session.bw
                            if c_link.link_id in final_paths_link_dict:
                                sess_bw += final_paths_link_dict[c_link.link_id] * session.bw
                            new_load = (c_link.used_cap + sess_bw) / float(c_link.cap)
                            if new_load > c_link_max_load:
                                self.overloaded_link.add(c_link_id)
                                loaded_link = True
                                break
                if loaded_link:
                    continue

                path_nodes = self._simple_paths_nodes[src, dst][path_idx]
                pkt_type_conflict = False
                if parent_pkt_type:
                    for n in path_nodes[1:]:
                        if parent_pkt_type in self._sp_inc_pkt_type[session.addr][n]:
                            # print 'XXX', n, parent_pkt_type
                            pkt_type_conflict = True
                            break
                if pkt_type_conflict:
                    if backtrace:
                        bt_str = 'XYXYXY Path: {%s} is rejected!' % ','.join(path_nodes)
                        print 'XXX Conflict: ', bt_str
                        backtrace.append(bt_str)
                    continue

                for node_idx, _ in enumerate(path_nodes):
                    last_srv_id, last_node_idx, base_srv_map = self._match_sf_to_path(path_nodes, node_idx, services)
                    if last_srv_id > max_service_len:
                        max_service_len = last_srv_id
                        service_path = None
                        print "previous current_cost = ", current_cost
                        current_cost = float('inf')
                        print "setting current cost to inf", current_cost

                    if last_srv_id == len(services):
                        found = True
                        x1 = time.time()
                        path_cost = self._calculate_path_cost(session, path_nodes, path, base_srv_map)
                        self.x1_total_time += time.time() - x1
                        next_path = path_nodes
                        next_services = []
                    else:
                        if not found:
                            x1 = time.time()
                            path_cost = self._calculate_path_cost(session, path_nodes[:last_node_idx+1],
                                                                  path[:last_node_idx], base_srv_map)
                            self.x1_total_time += time.time() - x1
                            next_path = path_nodes[:last_node_idx+1]
                            path = path[:last_node_idx]
                            # next_services = services[last_srv_id:]

                    if path_cost < current_cost and last_srv_id == max_service_len: # set service_path if cost lower and equal srv num, or more srv num
                        current_cost = path_cost
                        service_path = (path_cost, next_path, base_srv_map, path)
                        next_services = services[last_srv_id:] # set next_services if service_path is set

                    if len(services) == 0 or found:
                        break
                
                if found:
                    break

        no_service = len(services) > 0 and len(services) == len(next_services)

        # print session.addr, '->', dst, found, next_services
        # print 'no service', no_service
        if not no_service and service_path:
            final_paths.append(service_path)

        if no_service or len(next_services) > 0:
            next_src = src if no_service or not service_path else service_path[1][-1] # last node of next_path 
            # pick next node to be the source
            next_service = next_services[0]
            next_node_ids = self.app.get_nodes_ids_by_service(next_service)
            next_current_cost = float('inf')
            next_node = None
            next_path_links = None
            next_path_nodes = None
            next_srv_map = OrderedDict()
            next_max_services = []
            # print 'XXX', next_node_ids, next_service, next_services
            for next_node_id in next_node_ids:
                # print next_src, next_node_id
                if next_src != next_node_id:
                    next_max_services_ = self._get_max_services(next_services,
                                                               self.app.get_node(next_node_id).services_names)
                    for next_path_idx_, next_path_ in enumerate(self._simple_paths[next_src, next_node_id]):

                        # link so far in final_paths
                        final_paths_link_dict = defaultdict(int)
                        for p in final_paths:
                            for l in p[3]:
                                final_paths_link_dict[l.link_id] += 1
                        
                        # to satisfy link load constraints
                        path_key = (next_src, next_node_id)
                        loaded_link = False
                        if path_key in self._path_link_load_constraint_map:
                            if next_path_idx_ in self._path_link_load_constraint_map[path_key]:
                                constrained_link_ids = self._path_link_load_constraint_map[path_key][next_path_idx_]
                                for c_link_id in constrained_link_ids:
                                    c_link = self._links_map[c_link_id]
                                    c_link_max_load = c_link.constraint_map['load']
                                    sess_bw = session.bw
                                    if c_link.link_id in final_paths_link_dict:
                                        sess_bw += final_paths_link_dict[c_link.link_id] * session.bw
                                    
                                    new_load = (c_link.used_cap + sess_bw) / float(c_link.cap)

                                    if new_load > c_link_max_load:
                                        loaded_link = True
                                        break
                        if loaded_link:
                            continue

                        next_path_nodes_ = self._simple_paths_nodes[next_src, next_node_id][next_path_idx_]
                        # if next_path_idx_ > 20:
                        #     break
                        x1 = time.time()
                        next_path_cost_ = self._calculate_path_cost(session, next_path_nodes_, next_path_,
                                                                    {next_node_id: next_max_services_})
                        self.x1_total_time += time.time() - x1
                        total = 0
                        for n in next_path_nodes_:
                            total += self.commun[n][dst]
                        if  next_path_cost_  < next_current_cost:
                            next_current_cost = next_path_cost_
                            next_node = next_node_id
                            next_path_nodes = next_path_nodes_
                            next_path_links = next_path_
                            next_max_services = next_max_services_

            next_srv_map[next_node] = next_max_services
            next_service_path = (next_current_cost, next_path_nodes, next_srv_map, next_path_links)
            next_services = self._get_diff_services(next_services, next_max_services)
            final_paths.append(next_service_path)
            # print 'next_node', next_node, 'using', next_path_nodes, ', next services', next_services

            if next_path_nodes:
                self._find_sf_path(session, src=next_node, dst=dst, services=next_services, final_paths=final_paths)

    def _set_sp_sf_pkt_type(self, session, sf_paths):
        addr = session.addr
        for sf_path in sf_paths:
            for node_id, inc_pkt_type in sf_path.inc_pkt_type.items():
                if inc_pkt_type not in self._sp_inc_pkt_type[addr][node_id]:
                    self._sp_inc_pkt_type[addr][node_id].append(inc_pkt_type)

            for node_id, out_pkt_type in sf_path.out_pkt_type.items():
                if out_pkt_type not in self._sp_out_pkt_type[addr][node_id]:
                    self._sp_out_pkt_type[addr][node_id].append(out_pkt_type)

    def _sf_draw_graphviz(self, session, sf_paths):
        if not self._sp_draw_graph:
            return

        graph = self._sp_graphviz[session.addr]
        # graph.attr('node', shape='circle')
        for sf_path_idx, sf_path in enumerate(sf_paths):
            for link_idx, link in enumerate(sf_path.path_links):
                n1 = link.src
                n2 = link.dst
                g1 = n1
                g2 = n2

                if n1 == session.src:
                    srv_names = ' &#8594; '.join([str(x) for x in session.required_services])
                    dsts = ','.join([str(x) for x in session.dsts])
                    xlabel = '<<font color=\'red\'><b>%s</b></font> <br/> <i>%s</i>>' % (srv_names, dsts)
                    graph.node('%d' % n1, penwidth='2', color='black',
                               style='filled', fillcolor='lightgrey', shape='circle', xlabel=xlabel)
                else:
                    if n1 not in self._sp_joined_receivers[session.addr]:
                        graph.node('%d' % n1, shape='circle')
                    else:
                        if self._sp_first_dst[session.addr] == n1:
                            graph.node('%d' % n1, shape='star')
                        else:
                            graph.node('%d' % n1, shape='doublecircle')

                if n2 in self._sp_joined_receivers[session.addr]:
                    if self._sp_first_dst[session.addr] == n2:
                        graph.node('%d' % n2, shape='star')
                    else:
                        graph.node('%d' % n2, shape='doublecircle')
                else:
                    graph.node('%d' % n2, shape='circle')

                if n1 in sf_path.srv_map:
                    srv_name = '-'.join([str(x) for x in sf_path.srv_map[n1]])
                    graph.node('%d-(%s)' % (n1, srv_name), shape='diamond')
                    graph.edge('%d' % n1, '%d-(%s)' % (n1, srv_name))
                    g1 = '%d-(%s)' % (n1, srv_name)

                if link_idx == 0:
                    inc_srv = sf_path.inc_pkt_type[n1]
                    out_srv = sf_path.out_pkt_type[n1]
                    srv = self._get_diff_services(out_srv, inc_srv)
                    if srv:
                        srv_name = '-'.join([str(x) for x in srv])
                        graph.edge('%d-(%s)' % (n1, srv_name), '%d' % n2)
                    else:
                        graph.edge('%d' % n1, '%d' % n2)

                if n2 in sf_path.srv_map:
                    srv_name = '-'.join([str(x) for x in sf_path.srv_map[n2]])
                    graph.node('%d-(%s)' % (n2, srv_name), shape='diamond')
                    graph.edge('%d' % n2, '%d-(%s)' % (n2, srv_name))
                    graph.attr('node', shape='circle')

                if link_idx > 0:
                    graph.edge('%s' % g1, '%s' % g2)

    def _sf_render_graphviz(self, session):
        if not self._sp_draw_graph:
            return
        graph = self._sp_graphviz[session.addr]
        graph.render()

    def _find_simple_service_path(self, session, dst, tree):
        _debug = False # session.addr == '10.1.1.4' and dst == 34
        # if we haven't allocated any path for the session yet.
        if session.required_services:
            # TODO check for loops (?)
            paths = []
            paths_links = []

            srv_maps = []
            final_sf_paths = []
            if not self._sp_has_path[session.addr]:
                # First time to allocate paths to this session
                print session.addr, dst, session.required_services
                sf_paths = []
                self._find_sf_path(session, session.src, dst, session.required_services, final_paths=sf_paths)
                parent_sf_path = None
                for p in sf_paths:
                    if not p[1]:
                        return [], srv_maps, []
                    new_sf_path = _SFPath(cost=p[0], path_nodes=p[1], path_links=p[3], srv_map=p[2],
                                          parent_sf_path=parent_sf_path)
                    parent_sf_path = new_sf_path
                    final_sf_paths.append(new_sf_path)
                self._set_sp_sf_pkt_type(session, final_sf_paths)
                print 'First Path:'
                if len(final_sf_paths) > 1:
                    print 'HELLLOOO'
                for _sf_path in final_sf_paths:
                    print '(1) Path Nodes:', _sf_path.path_nodes
                    print '(2) Srv Map:', _sf_path.srv_map
                    print '(3) INC Pkt Type:', self._sp_inc_pkt_type[session.addr]
                    print '(4) OUT Pkt Type:', self._sp_out_pkt_type[session.addr]
                for p in sf_paths:
                    paths.append(p[1])
                    srv_maps.append(p[2])
                    paths_links.append(p[3])
                    self._update_sf_resources(session, path_links=p[3], path_nodes=p[1], srv_map=p[2])
                # self._sf_draw_graphviz(session, final_sf_paths)
            else:
                stop = False
                min_cost = float("inf")
                
                if session.required_services in self._sp_inc_pkt_type[session.addr][dst] or \
                        session.required_services in self._sp_out_pkt_type[session.addr][dst]:
                    self._sp_joined_receivers[session.addr].add(dst)
                    return paths, srv_maps, final_sf_paths

                checked_n = set()
                for sp_prev_path in reversed(self._sp_sf_solution[session.addr]):
                    if stop:  # or paths:
                        print 'BREAK!'
                        break
                    sp_path = sp_prev_path.path_nodes

                    for n in reversed(sp_path):
                        if n in checked_n:
                            continue
                        checked_n.add(n)
                        n_services = self._get_diff_services(session.required_services,
                                                             sp_prev_path.out_pkt_type[n])
                        parent_pkt_type = sp_prev_path.out_pkt_type[n]
                        if n == dst and session.required_services in self._sp_out_pkt_type[session.addr][dst]:
                            paths = []
                            srv_maps = []
                            paths_links = []
                            final_sf_paths = []
                            self._sp_joined_receivers[session.addr].add(dst)
                            stop = True
                            break
                        n_paths = []
                        self._find_sf_path(session, n, dst, n_services, final_paths=n_paths,
                                           parent_pkt_type=parent_pkt_type)

                        new_path_cost = sum(p[0] for p in n_paths)
                        print new_path_cost
                        if n_paths and new_path_cost < min_cost:
                            if _debug:
                                print 'XXX Resetting paths from', paths, 'to', n_paths
                            # stop = True
                            min_cost = new_path_cost
                            paths = []
                            srv_maps = []
                            paths_links = []
                            final_sf_paths = []
                            cur_sp_parent = sp_prev_path
                            for p in n_paths:
                                paths.append(p[1])
                                srv_maps.append(p[2])
                                paths_links.append(p[3])
                                print p[1], sp_prev_path.path_nodes
                                new_sf_path = _SFPath(cost=p[0], path_nodes=p[1], path_links=p[3], srv_map=p[2],
                                                      parent_sf_path=cur_sp_parent)
                                final_sf_paths.append(new_sf_path)
                                cur_sp_parent = new_sf_path
                        
                self._set_sp_sf_pkt_type(session, final_sf_paths)

                if len(final_sf_paths) > 1:
                    print 'HELLLOOO'

                for _sf_path in final_sf_paths:
                    print '(1) Path Nodes:', _sf_path.path_nodes
                    print '(2) Srv Map:', _sf_path.srv_map
                    print '(3) INC Pkt Type:', self._sp_inc_pkt_type[session.addr]
                    print '(4) OUT Pkt Type:', self._sp_out_pkt_type[session.addr]

                for p1, p2, p3 in zip(paths, srv_maps, paths_links):
                    self._update_sf_resources(session, path_links=p3, path_nodes=p1, srv_map=p2)

            return paths, srv_maps, final_sf_paths
        else:
            if not tree.has_path:
                # pick a path
                result = self._filter_simple_service_paths(session, session.src, dst, session.required_services)
                return result.path_nodes, result.srv_map
            else:
                # all simple paths allocated for this tree so far
                prev_node_paths = tree.get_node_paths()
                # for each of the prev simple paths, this list contains the node id for the last allocated service
                # if the path has no service, the node id is -1
                prev_last_srv_node = tree.get_node_paths_services()

                for last_srv_node_id in prev_last_srv_node:
                    if last_srv_node_id >= 0:
                        # print 'last_srv_node_id:', last_srv_node_id
                        last_srv_node = tree.get_node(last_srv_node_id)
                        traversed_nodes, _ = tree.traverse(last_srv_node)
                        for new_node_id in traversed_nodes:
                            # print 'down new_node_id', new_node_id
                            result = self._filter_simple_service_paths(session, src=new_node_id, dst=dst, services=[],
                                                                       ignore_nodes=tree.get_nodes_ids())
                            if result.path_nodes:
                                # print result.found, result.optimized
                                return result.path_nodes, result.srv_map
                        services = last_srv_node.services
                        ok_parent = last_srv_node.parent
                        while ok_parent is not None:
                            # print 'up new_node_id', ok_parent.node_id, '->', dst, services
                            result = self._filter_simple_service_paths(session, src=ok_parent.node_id, dst=dst,
                                                                       services=services, ignore_nodes=tree.get_nodes_ids())
                            if result.path_nodes:
                                # print result.found, result.optimized
                                return result.path_nodes, result.srv_map

                            if ok_parent.services:
                                services = ok_parent.services + services
                            ok_parent = ok_parent.parent

                final_result = SSPResult()
                min_cost = float("inf")
                for last_srv_node_id, prev_node_path in zip(prev_last_srv_node, prev_node_paths):
                    if last_srv_node_id < 0:
                        ls = None
                        reversed_node_path = reversed(prev_node_path)
                        # reversed_node_path = prev_node_path
                        if session.pass_map['nodes']:
                            pass_node_id = self._pass_map_src[session.addr]
                            ls = [pass_node_id] + list(tree.joined_receivers)
                        else:
                            ls = [ok_node.node_id for ok_node in reversed_node_path]

                        for node_id in ls:
                            # print '>>>', ok_node.node_id, '->', dst
                            result = self._filter_simple_service_paths(session, src=node_id, dst=dst, services=[],
                                                                       ignore_nodes=tree.get_nodes_ids())
                            if result.cost < min_cost:
                                min_cost = result.cost
                                final_result = result
                return final_result.path_nodes, final_result.srv_map


    def _regenerate_paths(self, s):
        copy_graph = nx.DiGraph()

        for link_id, _ in self.app.links.items():
            copy_graph.add_edge(link_id[0], link_id[1])

        # Remove edge with insufficient residual capacity
        for link_id, _ in self.app.links.items():
            if self._links_map[link_id].cap - (self._links_map[link_id].used_cap + s.bw ) < 0:
                copy_graph.remove_edge(link_id[0], link_id[1])
                self.overloaded_link.add(link_id)

        for link_id in self.overloaded_link:
            if link_id in copy_graph.edges():
                copy_graph.remove_edge(link_id[0], link_id[1])

        betweenness = nx.edge_betweenness_centrality(copy_graph)
        for k, l in self._links_map.items():
            if k in betweenness:
                l.betweeness_score = betweenness[k]
                print l

        pairs = []
        for l in self.overloaded_link:
            pairs += list(self._links_map_src_dst[l])
        
        self._links_map_src_dst = {(l.src, l.dst): set() for l in self.app.links.values()}

        if pairs:
            # Path Generation
            try:
                simple_paths_dict = calculate_simple_paths_regenerate(copy_graph, self._links_map, ppp=self._ppp,
                                                        pool_size=self._pool_size, pairs=pairs)
                for path_key, path_tuples in simple_paths_dict.iteritems():

                    self._simple_paths[path_key] = path_tuples[0]
                    self._simple_paths_nodes[path_key] = path_tuples[1]
                    self._simple_paths_links_ids[path_key] = path_tuples[2]

                    for p in self._simple_paths_links_ids[path_key]:
                        for link_id in p:
                            self._links_map_src_dst[link_id].add((path_key))

                    for path_idx, path_link_ids in enumerate(path_tuples[2]):
                        constrained_link_ids = set(path_link_ids).intersection(self._link_load_constraint_set)
                        if constrained_link_ids:
                            self._path_link_load_constraint_map[path_key][path_idx] = constrained_link_ids

                    for path_idx, path_node_ids in enumerate(path_tuples[1]):
                        constrained_node_ids = set(path_node_ids).intersection(self._node_tcam_constraint_set)
                        if constrained_node_ids:
                            self._path_node_tcam_load_constraint_map[path_key][path_idx] = constrained_node_ids
            except:
                print "Path Generation FAIL"

        self.overloaded_link = set()
        print '[oktopus] _regenerate_paths done'

    def pick_dst_comm(self, s_session, priority_list, ignore):
        if len(ignore) == 0:
            dst = (None, float('inf'))
            for k, v in self.commun[s_session.src].items():
                if k in s_session.dsts and dst[1] > v:
                    dst = (k, v)
            return dst[0]
        else:
            dst = (None, float('inf'))
            total = 0
            import collections
            dicc = collections.defaultdict(int)
            for n in ignore:
                for k, v in self.commun[n].items():
                    if k in s_session.dsts and dst[1] > v and k not in ignore:
                        dst = (k, v)
            return dst[0]
        


    def optimize(self):
        # sorted_sessions = self._sort_sessions()
        sorted_sessions = self.app.get_sessions()
        start_time = time.time()

        num_ssessios = 0

        for s_idx, s_session in enumerate(sorted_sessions, start=1):
            num_ssessios += 1
            print "Session::::",s_session, num_ssessios
            session_start_time = time.time()
            # print s_session
            if s_idx % 1000 == 0:
                print '[oktopus]', s_idx
            repeat = True
            trial_count = 1
            repeat_count = 3
            # destination that could not join in previous steps
            prev_non_joined = []
            repeat_whole = 2
            trial_whole_count = 1

            # print s_session.addr
            while repeat and trial_count <= repeat_count and trial_whole_count <= repeat_whole:
                tree = self._solution.get_tree(s_session)
                # destinations which were picked previously
                ignore_dsts = set()
                dst = None
                for _ in s_session.dsts:
                    # TODO maybe pick a dst based on how close/far it is from the src?
                    # Pick a destination which has simple paths that violate the delay constraints.
                    # Otherwise, use the pick_dst function.

                    invalid_delay = self._invalid_delay_map.get(s_session.addr)
                    if invalid_delay:
                        dst = invalid_delay.pop(0)
                    else:
                        # Pick a dst as follows:
                        # 1. don't pick a dst that is already in ignore_dsts
                        # 2. If prev_non_joined has destinations, pick the first one
                        # 3. Otherwise, pick a dst randomly from session.dsts
                        if s_session.required_services:
                            dst = self.pick_dst_comm(s_session, priority_list=prev_non_joined, ignore=self._sp_joined_receivers[s_session.addr])
                            # dst = pick_dst(s_session, priority_list=prev_non_joined, ignore=self._sp_joined_receivers[s_session.addr])

                            # print '>> Trial#:', trial_count, ', whole count', trial_whole_count, ', for dst:', dst, 'session ',s_session.addr
                        else:
                            dst = pick_dst(s_session, priority_list=prev_non_joined, ignore=ignore_dsts)
                    ignore_dsts.add(dst)

                    if not dst:
                        break

                    if s_session.required_services:
                        if not self._sp_first_dst[s_session.addr]:
                            self._sp_first_dst[s_session.addr] = dst
                        if dst not in self._sp_joined_receivers[s_session.addr] and dst:
                            ssp, srv_map, sf_sol = self._find_simple_service_path(s_session, dst, tree)
                            if sf_sol and any(_sf_sol.path_nodes[-1] == dst for _sf_sol in sf_sol):
                                # we found a path to dst
                                self._sp_has_path[s_session.addr] = True
                                self._sp_joined_receivers[s_session.addr].add(dst)
                                # self._sp_solution[s_session.addr].append(zip(ssp, srv_map))
                                for sf_path, path in zip(sf_sol, ssp):
                                    self._sp_sf_solution[s_session.addr].append(sf_path)
                                self._sf_draw_graphviz(s_session, sf_sol)
                    else:
                        if dst not in tree.joined_receivers and dst:
                            ssp, srv_map = self._find_simple_service_path(s_session, dst, tree)
                            if ssp:
                                self._solution.add_path(s_session, ssp, srv_map)
                repeat = not self._solution.is_session_done(s_session)
                if s_session.required_services:
                    repeat = len(set(s_session.dsts).difference(self._sp_joined_receivers[s_session.addr])) != 0
                trial_count += 1

                if s_session.required_services:
                    prev_non_joined = list(set(s_session.dsts).difference(self._sp_joined_receivers[s_session.addr])) + prev_non_joined
                else:
                    prev_non_joined = list(set(s_session.dsts).difference(tree.joined_receivers)) + prev_non_joined
            
                if not dst:
                    break
                
                if trial_count == repeat_count and repeat:

                    
                    session_links = []
                    if s_session.required_services:
                        # there is SC, from self_sp_sf_solution
                        session_links = []
                        for p in self._sp_sf_solution[s_session.addr]:
                            session_links += [tuple(int(n) for n in l.link_id.replace("(", "").replace(")", "").split('_')) for l in p.path_links]
                    else:
                        # no SC, from OkTree
                        tree =  self._solution.get_tree(s_session)
                        _, link_tuples = tree.traverse()
                        session_links = link_tuples
                    self.reverse_resources(solution=self._solution, session=s_session, links=session_links)
                    self._sp_joined_receivers[s_session.addr] = set()
                    prev_non_joined = []
                    self._sp_sf_solution[s_session.addr] = []
                    ignore_dsts = set()
                    self._sp_first_dst[s_session.addr] = None
                    self._solution.get_new_tree(s_session)
                    self._sp_has_path[s_session.addr] = False

                    self._sp_inc_pkt_type[s_session.addr] = defaultdict(list)
                    self._sp_out_pkt_type[s_session.addr] = defaultdict(list)
                    for n in self.app.get_nodes():
                        self._sp_pkt_type[s_session.addr][n.node_id] = []

                    self._sp_has_path[s_session.addr] = False
                    self._sp_solution[s_session.addr] = []

                    # print "self.overloaded_link", self.overloaded_link
                    self._regenerate_paths(s_session)

                    trial_count = 1
                    trial_whole_count += 1

            if s_session.required_services:
                print '*'*80
            self.update_resources(solution=self._solution, session=s_session)
            session_links = []
            if s_session.required_services:
                # there is SC, from self_sp_sf_solution
                session_links = []
                for p in self._sp_sf_solution[s_session.addr]:
                    session_links += [tuple(int(n) for n in l.link_id.replace("(", "").replace(")", "").split('_')) for l in p.path_links]
            else:
                # no SC, from OkTree
                tree =  self._solution.get_tree(s_session)
                _, link_tuples = tree.traverse()
                session_links = link_tuples
            print "s_session.addr", s_session.addr

            if s_session.required_services and len(set(s_session.dsts).difference(self._sp_joined_receivers[s_session.addr])) != 0:
                self.reverse_resources(solution=self._solution, session=s_session, links=session_links)
                continue

            for h, t in session_links:
                print h, t
            curr_time = time.time() - session_start_time
            self.update_session_cost(solution=self._solution, session=s_session,  links=session_links, time=curr_time)

        total_time = time.time() - start_time
        self._solution.total_time = total_time
        print 'All DONE:', ( sum( [len(self._sp_joined_receivers[s.addr]) == len(s.dsts) for s in self.app.get_sessions() if s.required_services] ) + sum([self._solution.is_session_done(s) for s in self.app.get_sessions() if not s.required_services]) ) / float(len(sorted_sessions))
        # print 'All DONE WITH SERVICES:', all(len(self._sp_joined_receivers[s.addr]) == len(s.dsts) for s in self.app.get_sessions() if s.required_services)
        
        for s in self.app.get_sessions():
            if s.required_services:
                self._sf_render_graphviz(s)

        return self._solution
        