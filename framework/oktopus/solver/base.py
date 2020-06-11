

class Solver:
    def __init__(self, name, app, **kwargs):
        self.name = name
        self.app = app
        self.options = kwargs
        self.cache_dir = self.options.pop('ok_cache_dir', None)
        links = self.app.links.values()
        self._links_map = {(l.src, l.dst): l for l in links}
        self.allo_order = 0

    def update_resources(self, solution, session, link_usage_fn=None):
        tree = solution.get_tree(session)
        if not tree.has_path:
            return

        nodes_ids = tree.get_nodes_ids()
        links_ids = tree.get_links_ids()

        for node_id in nodes_ids:
            ok_node = tree.get_node(node_id)
            node = self.app.get_node(node_id)
            fn_dict = self.app.routing.node_usage_fn_map[node_id]
            for srv_name in node.services:
                srv = node.get_service(srv_name)
                if not srv.ordered and node_id in tree.last_nodes_ids:
                    continue

                required_res_dict = session.res.get(srv_name, None)
                if required_res_dict:
                    # print srv, required_res_dict
                    for res_name, res_value in required_res_dict.iteritems():
                        if res_name in srv.resources_cap:
                            fn = fn_dict[srv_name][res_name]
                            fn(node, session, srv_name, res_name)
                            # print srv_name, res_name, srv.used_cap[res_name], srv.resources_cap[res_name]

        for link_id in links_ids:
            link = self._links_map[link_id]
            fn = link_usage_fn if link_usage_fn else self.app.routing.link_usage_fn_map[link.link_id]
            fn(link, session)

    def update_session_cost(self, solution, session, links, time=-1):
        sess_cost = 0
        for l in links:
            sess_cost += self._links_map[l].bw_cost * session.bw
        session.cost = sess_cost       
        session.time = time    
        self.allo_order += 1
        session.num_allo = self.allo_order

    def reverse_resources(self, solution, session, links):
        for link_id in links:
            link = self._links_map[link_id]
            link.used_cap -= session.bw
            

    def optimize(self):
        raise NotImplementedError('Please use a specialized class...')
