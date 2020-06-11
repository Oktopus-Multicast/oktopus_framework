from cpython cimport bool

import time
from copy import deepcopy
from collections import OrderedDict

import networkx as nx

from base import Solver
from solution import Solution
from oktopus_utils import pick_dst
from ..multicast.session import Session

MAX_PATH_COST = 10 ** 12


cdef class SSPResult:
    def __init__(self, found=False, cost=MAX_PATH_COST, path=None, path_nodes=None, srv_map=None, optimized=False):
        self.found = found
        self.cost = cost
        self.path = path
        self.path_nodes = path_nodes
        self.srv_map = srv_map
        self.optimized = optimized


cdef class CyOktopusSolver:
    def __init__(self, app, **kwargs):
        self.name = 'oktopus'
        self.app = app
        self.options = kwargs

        self._solution = Solution()
        self._cutoff = 0

        # oktopus data structures
        self._simple_paths = {}
        self._simple_paths_nodes = {}
        self._simple_paths_links_ids = {}
        self._links_map = {}
        # per session per node
        self._delay_map = {}
        self._invalid_delay_map = {}
        self._pass_map_src = {}

        self._init_oktopus()

    cdef void _init_oktopus(self):
        nodes_count = len(self.app.get_nodes())
        self._cutoff = int(self.options.pop('cutoff', nodes_count / 2.))
        if self._cutoff < 0 or self._cutoff > nodes_count:
            self._cutoff = int(nodes_count / 2.)
        print '[oktopus] using cutoff=', self._cutoff
        links = self.app.links.values()
        self._links_map = {(l.src, l.dst): l for l in links}
        self._init_simple_paths()
        self._init_delay_maps()

    cdef void _init_delay_maps(self):
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

    def _init_simple_paths(self):
        graph = self.app.graph
        nodes_ids = self.app.nodes.keys()
        # FIXME if we have path_key n1,n2: to get paths of n2,n1 we just need to reverse paths of n1,n2
        for n1 in nodes_ids:
            for n2 in nodes_ids:
                if n1 != n2:
                    path_key = n1, n2
                    self._simple_paths[path_key] = []
                    self._simple_paths_nodes[path_key] = []
                    self._simple_paths_links_ids[path_key] = []
                    for path in list(nx.all_simple_paths(graph, str(n1), str(n2), cutoff=self._cutoff)):
                        int_path = [int(n) for n in path]
                        links = self._construct_links_from_path(path)
                        links_ids = [(l.src, l.dst) for l in links]
                        self._simple_paths[path_key].append(links)
                        self._simple_paths_nodes[path_key].append(int_path)
                        self._simple_paths_links_ids[path_key].append(links_ids)

    def _construct_links_from_path(self, path):
        """ Given a path of servers, returns the list of links along this path
        :param path: list of servers
        :return: list of links
        """
        i = 0
        j = 1
        links = []
        while j < len(path):
            link_key = (int(path[i]), int(path[j]))
            links.append(self._links_map[link_key])
            i += 1
            j += 1
        return links

    @classmethod
    def _sort_session_key(cls, session, max_bw):
        assert isinstance(session, Session)
        # session_value = session.bw * len(session.dsts)
        session_value = (session.bw / float(max_bw)) * len(session.dsts)
        if session.constraint_map['load'] > 0:
            session_value *= 5
        if session.constraint_map['delay'] > 0:
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

    def _calculate_link_cost(self, session, link):
        load_cost = 1
        delay_cost = 1
        if self.app.routing.obj_min_mlu:
            fn = self.app.routing.link_cost_fn_map.get(link.link_id)
            if fn:
                load_cost = fn(link, session)
            else:
                print 'link cost: no fn'

        if self.app.routing.obj_min_delay:
            delay_cost = link.delay

        # if not obj_min_mlu and not obj_min_delay, link cost = 1
        # otherwise, either both of the costs are multiplied, or one of them is returned
        return load_cost * delay_cost

    def _calculate_node_cost(self, session, node, service):
        path_cost = 0
        if self.app.routing.obj_min_mnu:
            fn_dict = self.app.routing.node_cost_fn_map.get(node.node_id)
            if fn_dict:
                srv_name = service.name
                required_res_dict = session.res[srv_name]
                for res_name, res_value in required_res_dict.iteritems():
                    if res_name in service.resources_cap:
                        fn = fn_dict[srv_name][res_name]
                        path_cost += fn(node, session, srv_name, res_name)
                    else:
                        print 'node cost: service has no resource'
                        path_cost += MAX_PATH_COST
                return path_cost
            else:
                print 'node cost: no fn_dict'
        return 0

    def _calculate_links_costs(self, session, links):
        path_cost = 0
        # links cost
        for link in links:
            if link:
                path_cost += self._calculate_link_cost(session, link)
            else:
                raise ValueError('Not a valid link')
        return path_cost

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

        path_cost = self._calculate_links_costs(session, links)
        path_cost += self._calculate_nodes_costs(session, node_ids, srv_dict)
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

    def _update_resources(self, solution, session):
        tree = solution.get_tree(session)
        if not tree.has_path:
            return

        nodes_ids = tree.get_nodes_ids()
        links_ids = tree.get_links_ids()

        for node_id in nodes_ids:
            ok_node = tree.get_node(node_id)
            node = self.app.get_node(node_id)
            fn_dict = self.app.routing.node_usage_fn_map[node_id]
            for srv_name in ok_node.services:
                srv = node.get_service(srv_name)
                required_res_dict = session.res[srv_name]
                for res_name, res_value in required_res_dict.iteritems():
                    if res_name in srv.resources_cap:
                        fn = fn_dict[srv_name][res_name]
                        fn(node, session, srv_name, res_name)

        for link_id in links_ids:
            link = self._links_map[link_id]
            fn = self.app.routing.link_usage_fn_map[link.link_id]
            fn(link, session)

    def _filter_simple_service_paths(self, session, src, dst, services, ignore_nodes=None):
        all_dsts = session.dsts
        other_dsts = set(all_dsts).difference({dst, src})
        ignore_nodes_set = set()
        if ignore_nodes:
            ignore_nodes_set = set(ignore_nodes).difference({src})

        min_cost = MAX_PATH_COST
        min_cost_path = None
        min_cost_path_nodes = None
        min_cost_node_srv_map = None

        min_valid_cost = MAX_PATH_COST
        min_cost_valid_path = None
        min_cost_valid_path_nodes = None
        min_cost_valid_node_srv_map = None

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

            # to support avoid(links) API
            if path_links_ids_set.difference(session.avoid_map['links']) != path_links_ids_set:
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
            tree = self._solution.get_tree(session)
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
                min_node_cost = MAX_PATH_COST

                for node_idx, _ in enumerate(path_nodes):
                    node_srv_map_list = []
                    base_srv_map = self._generate_base_service_path(path_nodes, node_idx, services, max_srv_idx)

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
            return SSPResult(found=True, cost=min_valid_cost, path=min_cost_valid_path,
                             path_nodes=min_cost_valid_path_nodes, srv_map=min_cost_valid_node_srv_map, optimized=True)

        found = False
        optimized = False
        if min_cost_path_nodes:
            found = True
            optimized = False

        return SSPResult(found=found, cost=min_cost, path=min_cost_path, path_nodes=min_cost_path_nodes,
                         srv_map=min_cost_node_srv_map, optimized=optimized)

    cdef tuple _find_simple_service_path(self, session, int dst, tree):
        if not tree.has_path:
            # pick a path
            result = self._filter_simple_service_paths(session, session.src, dst, session.required_services)
            return result.path_nodes, result.srv_map
        else:
            prev_node_paths = tree.get_node_paths()
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
                            return result.path_nodes, result.srv_map
                    services = last_srv_node.services
                    ok_parent = last_srv_node.parent
                    while ok_parent is not None:
                        # print 'up new_node_id', ok_parent.node_id, '->', dst, services
                        result = self._filter_simple_service_paths(session, src=ok_parent.node_id, dst=dst,
                                                                   services=services, ignore_nodes=tree.get_nodes_ids())
                        if result.path_nodes:
                            return result.path_nodes, result.srv_map

                        if ok_parent.services:
                            services = ok_parent.services + services
                        ok_parent = ok_parent.parent

            final_result = SSPResult()
            for last_srv_node_id, prev_node_path in zip(prev_last_srv_node, prev_node_paths):
                min_cost = MAX_PATH_COST
                if last_srv_node_id < 0:
                    ls = None
                    reversed_node_path = reversed(prev_node_path)
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

    cpdef object optimize(self):
        cdef list sorted_sessions = self._sort_sessions()
        cdef double start_time = time.time()
        cdef int repeat_count = 10
        cdef:
            bool repeat
            int trial_count
            list prev_non_joined
            int dst
        for s_session in sorted_sessions:
            repeat = True
            trial_count = 1
            prev_non_joined = []
            while repeat and trial_count <= repeat_count:
                tree = self._solution.get_new_tree(s_session)
                ignore_dsts = set()
                for _ in s_session.dsts:
                    # TODO maybe pick a dst based on how close/far it is from the src?
                    invalid_delay = self._invalid_delay_map.get(s_session.addr)
                    if invalid_delay:
                        dst = invalid_delay.pop(0)
                    else:
                        dst = pick_dst(s_session, priority_list=prev_non_joined, ignore=ignore_dsts)
                    ignore_dsts.add(dst)
                    if dst not in tree.joined_receivers:
                        ssp, srv_map = self._find_simple_service_path(s_session, dst, tree)
                        if ssp:
                            self._solution.add_path(s_session, ssp, srv_map)
                repeat = not self._solution.is_session_done(s_session)
                trial_count += 1
                prev_non_joined = list(set(s_session.dsts).difference(tree.joined_receivers)) + prev_non_joined
            self._update_resources(solution=self._solution, session=s_session)
        cdef double total_time = time.time() - start_time
        self._solution.total_time = total_time
        return self._solution
