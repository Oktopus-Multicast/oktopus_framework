import networkx as nx

from ..dataset import read_isp_graph, parse_objects, OK_NODES_SERVICES_FILE
from ..solver import OktopusSolver, RSVPSolver, MLDPSolver, MTRSASolver, MSASolver, CPLEXMTESolver, CPLEXSCSolver
from ..solver import SRMcastRoutingTechnology, OFRoutingTechnology
from ..solver import ALGO_MAP
from network import Node, Link


class App:
    """
    App class exposes the Application API. 
    
    The Application API allows the operator to control different aspects of an application such as topology, services, session, routing, and solution.

    Attributes
    ----------
        name : str
            The name of the application.
        technology : str
            The name of the controller technologies.
        routing: Routing
            Routing object.
        sessions: dict
            Application sessions object.
        nodes: dict
            Network node object.
        links: dict
            Network link object.

    """

    def __init__(self, name, topo, technology='sr', node_file=None, parse_services=False):
        """
        Parameters
        ----------
        name : str
            The name of the application.
        topo : networkx.Graph or str
            The name or object of the network topology.
        technology : str
            The name of the controller technologies.
        node_file: str
            The file containing the network nodes.
        parse_services: bool
            To not parse the network service in the given network topology.
        """

        self.name = name
        self.topo = topo
        self.technology = 'sr' if not technology else technology

        self.topo_cache_dir = None
        self.graph = None
        self.routing = None

        self.sessions = {}
        self.nodes = {}
        self.links = {}

        self._get_nodes_by_service_cache = {}
        self._get_nodes_ids_by_service_cache = {}
        self._init_app(parse_services=parse_services, node_file=node_file)

    def _init_app(self, parse_services=False, node_file=None):
        assert isinstance(self.technology, str) and self.technology in ['sr', 'sdn']
        self._parse_topo_file(parse_services=parse_services, node_file=node_file)

    def _parse_topo_file(self, parse_services=False, node_file=None):
        if isinstance(self.topo, str):
            self.graph = read_isp_graph(self.topo)
        elif isinstance(self.topo, nx.Graph):
            self.graph = self.topo

        self._create_nodes(parse_services=parse_services, node_file=node_file)
        self._create_links()

    def _create_nodes(self, parse_services=False, node_file=None):
        service_map = {}

        if parse_services:
            if not node_file:
                print 'Node file is not set'
            else:
                nodes = parse_objects(node_file, Node)
                for n in nodes:
                    service_map[int(n.node_id)] = [x.name for x in n.get_services()]

        for node_id, node_data in self.graph.node.iteritems():
            lat = node_data.get('Latitude', 0.)
            lon = node_data.get('Longitude', 0.)
            new_node = Node(node_id=int(node_id), lat=lat, lon=lon,
                            services_names=service_map.get(int(node_id), None)
                            )
            self.nodes[new_node.node_id] = new_node

    def _create_links(self):
        nodes = self.nodes.values()
        traversed_edges = []
        for node1 in nodes:
            node_edges = self.graph.edges(node1.node_id, data=True)
            for _, neighbor, edge_data in node_edges:
                tmp_nodes = [n for n in nodes if n.node_id == int(neighbor)]
                if tmp_nodes and len(tmp_nodes) == 1:
                    node2 = tmp_nodes[0]
                    if (node1.node_id, node2.node_id) not in traversed_edges:
                        link = Link(link_id='({0}_{1})'.format(node1.node_id, node2.node_id),
                                    src=node1.node_id, dst=node2.node_id,
                                    cap=edge_data.get('Capacity', 0.),
                                    igp_weight=edge_data.get('IGPWeight', -1),
                                    distance=edge_data.get('Distance', 0.),
                                    delay=edge_data.get('Delay', 0.),
                                    port1=edge_data.get('src_port', -1),
                                    port2=edge_data.get('dst_port', -1))
                        traversed_edges.append((node1.node_id, node2.node_id))
                        self.links[node1.node_id, node2.node_id] = link

    def add_sessions(self, sessions):
        """Add application sessions.

        Parameters
        ----------
        sessions: dict
            Application sessions object.
        """

        for s in sessions:
            self.sessions[s.addr] = s

    def get_sessions(self):
        """Get application sessions.

        Returns
        ----------
            list
                A list of application sessions.
        """

        return self.sessions.values()

    def get_session(self, addr):
        """Get an application session given the session address.

        Parameters
        ----------
        addr: str
            IP address of the application session.

        Returns
        -------
        Session
            The application session.
        """

        return self.sessions.get(addr)

    def get_links(self):
        """Get network links.

        Returns
        -------
        Link
            A list of network links.
        """

        return self.links.values()

    def get_nodes(self):
        """Get network nodes.

        Returns
        -------
        Node
            A list of network nodes.
        """

        return self.nodes.values()

    def get_link(self, src, dst):
        """Get network given the connected network nodes.

        Parameters
        ----------
        src: str
            The network ID of the source network node.
        dst: str
            The network ID of the destination network node.

        Returns
        -------
        Link
            The network link.
        """

        link_key = (src, dst)
        return self.links.get(link_key)

    def get_node(self, node_id):
        """Get network node object give the ID.

        Parameters
        ----------
        node_id: str
            The network ID of the network node.

        Returns
        -------
        Node
            The network node.
        """

        return self.nodes.get(node_id)

    def get_nodes_by_service(self, srv_name):
        """Get network nodes containing the given network service.

        Parameters
        ----------
        srv_name: str
            The name of the network service.

        Returns
        -------
        list
            A list of network nodes.
        """

        if srv_name not in self._get_nodes_by_service_cache:
            nodes = []
            for node in self.get_nodes():
                srv_map = node.get_services_map()
                if srv_name in srv_map:
                    nodes.append(node)
            self._get_nodes_by_service_cache[srv_name] = nodes
        return self._get_nodes_by_service_cache[srv_name]

    def get_nodes_ids_by_service(self, srv_name):
        """Get network nodes IDs containing the given network service.

        Parameters
        ----------
        srv_name: str
            The name of the network service.

        Returns
        -------
        list
            A list of network nodes IDs.
        """

        if srv_name not in self._get_nodes_ids_by_service_cache:
            nodes_ids = []
            for node in self.get_nodes():
                srv_map = node.get_services_map()
                if srv_name in srv_map:
                    nodes_ids.append(node.node_id)
            self._get_nodes_ids_by_service_cache[srv_name] = nodes_ids
        return self._get_nodes_ids_by_service_cache[srv_name]

    def print_services(self):
        """Print network services deployed on the network."""

        for node in self.get_nodes():
            print node
            for srv in node.get_services():
                print '\t', srv

    def set_routes(self, routes):
        """Set the Routing object to the application.

        The Routing object is used to define routing costs, constraints and objectives of the application.

        Parameters
        ----------
        routes: Routing
            The Routing object.
        """

        self.routing = routes
        self.routing.init_maps(self)

    def _set_cache_dir(self, cache_dir):
        self.topo_cache_dir = cache_dir

    def solve(self, algorithm='oktopus', **kwargs):
        """Run the specified algorithm to solve the application.

        Parameters
        ----------
        algorithm: str
            The optimization engine algorithm.
        """

        assert isinstance(algorithm, str) and algorithm in ALGO_MAP

        # determine solver and technology classes
        tech_cls = SRMcastRoutingTechnology
        solver_cls = ALGO_MAP[algorithm]

        if self.technology == 'sdn':
            tech_cls = OFRoutingTechnology

        kwargs['ok_cache_dir'] = self.topo_cache_dir
        # find a solution for the network application
        solver = solver_cls(self, **kwargs)
        solution = solver.optimize()
        self._post_solution(solution)

        # encode the solution to the corresponding routing technology
        tech = tech_cls(solution)
        tech.encode()

        # self._check_constraints(solution)
        # self._print_solution(solution, details=False)
        self._print_solution(solution, details=True)

    def _post_solution(self, solution):
        # set session.delay, session.load and session.max_hops
        for addr, tree in solution.trees.iteritems():
            session = self.get_session(addr)
            delay_map = {session.src: 0}
            max_load = 0
            max_hops = tree.get_max_hop_count()
            _, link_tuples = tree.traverse()
            if link_tuples:
                for link_tuple in link_tuples:
                    src = link_tuple[0]
                    dst = link_tuple[1]
                    link = self.get_link(src, dst)
                    link_load = link.get_load()
                    if link_load > max_load:
                        max_load = link_load
                    delay_map[dst] = link.delay + delay_map[src]
            delay_list = [delay_map[dst] for dst in session.dsts if dst in tree.joined_receivers]
            if delay_list:
                session.delay = max(delay_list)
            else:
                session.delay = -1
            session.load = max_load
            session.max_hops = max_hops

    def _check_constraints(self, solution):
        # TODO check service chaining, pass nodes
        for addr, tree in solution.trees.iteritems():
            session = self.get_session(addr)
            nodes_ids, link_tuples = tree.traverse()
            nodes_ids = set(nodes_ids)
            link_tuples = set(link_tuples)
            str_list = []

            if not solution.is_session_done(session):
                str_list.append('X Non-joined destinations: {}'.
                                format(set(session.dsts).difference(tree.joined_receivers)))

            if session.avoid_map['links']:
                if link_tuples.difference(session.avoid_map['links']) != link_tuples:
                    str_list.append('X avoid links')

            if session.avoid_map['nodes']:
                if nodes_ids.difference(session.avoid_map['nodes']) != nodes_ids:
                    str_list.append('X avoid nodes')

            if session.avoid_map['sessions']:
                avoid_nodes = set()
                avoid_links = set()
                for other_addr in session.avoid_map['sessions']:
                    other_session = self.get_session(other_addr)
                    if other_session and solution.is_session_done(other_session):
                        other_tree = solution.get_tree(other_session)
                        other_nodes_ids = other_tree.get_nodes_ids()
                        other_links_ids = other_tree.get_links_ids()
                        avoid_nodes = avoid_nodes.union(other_nodes_ids)
                        avoid_links = avoid_links.union(other_links_ids)
                if nodes_ids.difference(avoid_nodes) != nodes_ids:
                    str_list.append('X avoid sessions - nodes')
                if link_tuples.difference(avoid_links) != link_tuples:
                    str_list.append('X avoid sessions - links')

            if str_list:
                print session
                for str_item in str_list:
                    print str_item

    def _print_solution(self, solution, details=True):
        if details:
            for addr, tree in solution.trees.iteritems():
                session = self.get_session(addr)
                print session, session.load, session.delay
                _, link_tuples = tree.traverse()

                print "Session Cost: {}, {}, {}, {}".format(session.addr, session.cost, session.time, session.num_allo)

                # if link_tuples:
                #     for link_tuple in link_tuples:
                #         src = link_tuple[0]
                #         dst = link_tuple[1]
                #         node = tree.get_node(src)
                #         if node.services:
                #             srv_str = '({} ({}), {})'.format(src, '&'.join(node.services), dst)
                #             print srv_str,
                #         else:
                #             print link_tuple,
                #     print
                #     print '*' * 80

        links = self.get_links()
        max_delay_session = max(self.get_sessions(), key=lambda s: s.delay)
        max_load_session = max(self.get_sessions(), key=lambda s: s.load)
        sorted_links = sorted(links, key=lambda l: l.get_load(), reverse=True)

        max_load = 0
        max_load_link = None
        total_routing_cost = 0
        for link in self.get_links():
            link_load = link.get_load()
            total_routing_cost += link.used_cap * link.bw_cost

            if link_load > max_load:
                max_load = link_load
                max_load_link = link
        print
        print 'Solution Stats:'
        print '-' * len('Solution Stats:')
        print 'Optimization time       =', solution.total_time, 'sec'
        print 'Max. loaded link        =', max_load_link
        print 'Max. link load value    =', 100 * max_load
        print 'Session with max. delay =', max_delay_session, ', delay =', max_delay_session.delay, ', load =', 100 * max_delay_session.load
        print 'Session with max. load  =', max_load_session, ', delay =', max_load_session.delay, ', load =', 100 * max_load_session.load
        print 'Total bandwidth cost    =', total_routing_cost

        print 'Link load (by load):'
        for link in sorted_links:
            link_load = 100 * link.get_load()
            print '\t', '{:10.4f}'.format(link_load), link

        # sample_session = self.get_session('10.1.0.191')
        # print sample_session
        # link_3_1 = self.get_link(3, 1)
        # print link_3_1.get_load(), link_3_1

