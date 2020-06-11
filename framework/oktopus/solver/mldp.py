import time
from math import ceil
from random import uniform

import networkx as nx

from base import Solver
from solution import Solution


class MLDPSolver(Solver):

    METRICS = ['hop-by-hop', 'delay']

    def __init__(self, app, **kwargs):
        r"""Creates multicast LDP solver for the network application.

        :param app:
            the App object
        :type app: ``App``
        :param \**kwargs:
            See below

        :Keyword Arguments:
            * *metric* (``str``) --
              The chosen metric to define the best path, the available options are *delay* or *hop-by-hop*.
              Default = delay
        """
        Solver.__init__(self, name='mldp', app=app, **kwargs)

        self._metric = ''
        self._mldp_graph = None

        # all shortest paths map
        self._all_sp_nodes_map = {}
        self._init_mldp()

    def _init_mldp(self):
        self._metric = self.options.pop('metric', 'delay')
        if self._metric not in self.METRICS:
            self._metric = 'delay'
        print '[mLDP] using metric:', self._metric
        self._init_mldp_graph()
        self._init_all_sp_map()

    def _init_mldp_graph(self):
        self._mldp_graph = nx.DiGraph()
        for link in self.app.get_links():
            if self._metric == 'delay':
                self._mldp_graph.add_edge(link.src, link.dst, weight=ceil(link.delay))
            elif self._metric == 'hop-by-hop':
                self._mldp_graph.add_edge(link.src, link.dst)
            elif self._metric == 'igp' and link.igp_weight >= 0:
                self._mldp_graph.add_edge(link.src, link.dst, weight=ceil(link.igp_weight))
            else:
                raise ValueError('Only delay or hop-by-hop are the available metrics for mLDP')

    def _init_all_sp_map(self):
        nodes_ids = self.app.nodes.keys()
        # TODO support both hop counts and min-delay
        for n1 in nodes_ids:
            for n2 in nodes_ids:
                if n1 != n2:
                    path_key = n1, n2
                    self._all_sp_nodes_map[path_key] = []
                    if self._metric == 'delay':
                        all_sps = [p for p in nx.all_shortest_paths(self._mldp_graph, n1, n2, weight='weight')]
                    elif self._metric == 'hop-by-hop':
                        all_sps = [p for p in nx.all_shortest_paths(self._mldp_graph, n1, n2)]
                    else:
                        raise ValueError('Only delay or hop-by-hop are the available metrics for mLDP')
                    if all_sps:
                        for sp in all_sps:
                            int_sp = [int(n) for n in sp]
                            self._all_sp_nodes_map[path_key].append(int_sp)

    @classmethod
    def _pick_sp(cls, shortest_paths):
        items = [(sp, 1. / float(len(shortest_paths))) for sp in shortest_paths]
        x = uniform(0, 1)
        cumulative_probability = 0.0
        selected_sp = None
        for item in items:
            cumulative_probability += item[1]
            if x < cumulative_probability:
                selected_sp = item[0]
                break
        return selected_sp

    def _update_resources(self, solution, session):
        tree = solution.get_tree(session)
        if not tree.has_path:
            return

        # nodes_ids = tree.get_nodes_ids()
        links_ids = tree.get_links_ids()

        # for node_id in nodes_ids:
        #     ok_node = tree.get_node(node_id)
        #     node = self.app.get_node(node_id)
        #     fn_dict = self.app.routing.node_usage_fn_map[node_id]
        #     for srv_name in ok_node.services:
        #         srv = node.get_service(srv_name)
        #         required_res_dict = session.res[srv_name]
        #         for res_name, res_value in required_res_dict.iteritems():
        #             if res_name in srv.resources_cap:
        #                 fn = fn_dict[srv_name][res_name]
        #                 fn(node, session, srv_name, res_name)

        for link_id in links_ids:
            src = int(link_id[0])
            dst = int(link_id[1])
            link = self.app.get_link(src, dst)
            fn = self.app.routing.link_usage_fn_map[link.link_id]
            fn(link, session)

    def optimize(self):
        solution = Solution()
        sessions = self.app.get_sessions()
        start_time = time.time()
        session_links = set()
        for session in sessions:
            session_start_time = time.time()
            src = session.src
            for dst in session.dsts:
                if solution.is_joined(session, dst):
                    continue
                sp_key = (src, dst)
                sp_list = self._all_sp_nodes_map.get(sp_key)
                if sp_list:
                    sp = self._pick_sp(shortest_paths=sp_list)
                    solution.add_path(session, sp, node_srv_map=None)
                    for i in range(len(sp)-1):
                        session_links.add((sp[i], sp[i+1]))
            self.update_resources(solution=solution, session=session)
            curr_time = time.time() - session_start_time
            self.update_session_cost(solution=solution, session=session,  links=session_links, time=curr_time)

        end_time = time.time() - start_time
        solution.total_time = end_time
        print 'All DONE:', all([solution.is_session_done(session) for session in sessions])

        mlu = 0
        for link in self.app.get_links():
            lu = link.used_cap / float(link.cap)
            if mlu < lu:
                mlu = lu
        print 'MLU = ', 100 * mlu
        print 'Total time =', end_time, 'sec.'

        # for addr, tree in solution.trees.iteritems():
        #     print self.app.get_session(addr)
        #     nn, ll = tree.traverse()
        #     if ll:
        #         print addr
        #         for l in ll:
        #             print l,
        #         print

        return solution
