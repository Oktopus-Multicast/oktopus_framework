from defaults import default_link_cost_fn, default_link_usage_fn, routing_cost_link_cost_fn
from defaults import default_node_cost_fn, default_node_usage_fn


class Routing:
    """
    Routing class exposes the Routing API. 

    The operator uses this API by creating and modifying a routing object. 
    Unlike the Session API, the Routing API defines node and link constraints 
    and network-wide objectives. Moreover, it allows the operator to change how 
    Oktopus calculates costs and consumes network resources by implementing cost 
    and usage functions.
    
    Attributes
    ----------
        obj_min_mlu : bool
            The objective is to minimize maximum link load.
        obj_min_mnu : bool
            The objective is to minimize maximum node load.
        obj_min_delay : bool
            The objective is to minimize the delay.
        obj_min_routing_cost : bool
            The objective is to minimize the routing cost.
        link_cost_fn_map : dict
            Custom cost function for network links. 
        link_usage_fn_map : dict
            Custom usage function for network links. 
        link_constraint_map : dict
            Custom constraint function for network links.
        node_cost_fn_map : dict
            Custom cost function for network nodes. 
        node_usage_fn_map : dict
            Custom usage function for network nodes. 
        node_constraint_map : dict
            Custom constraint function for network nodes.
    """
    
    def __init__(self):

        self.obj_min_mlu = False
        self.obj_min_mnu = False
        self.obj_min_delay = False
        self.obj_min_routing_cost = False

        self._links = None
        self.link_cost_fn_map = {}
        self.link_usage_fn_map = {}
        self.link_constraint_map = {}

        self.node_cost_fn_map = {}
        self.node_usage_fn_map = {}
        self.node_constraint_map = {}

    def init_maps(self, app):
        """
        Initialize Routing attributes.

        Parameters
        ----------
        app : App
            The App object.
        """

        self._links = app.get_links()
        nodes = app.get_nodes()
        for link in self._links:
            self._add_link_cost_fn(link, fn=None, override=False)
            self._add_link_usage_fn(link, fn=None, override=False)

        for node in nodes:
            services = node.get_services()
            for srv in services:
                for res_name in srv.resources_cap:
                    self._add_node_cost_fn(node, srv, res_name, None, override=False)
                    self._add_node_usage_fn(node, srv, res_name, None, override=False)

        if self._links and self.obj_min_routing_cost:
            for link in self._links:
                self.add_link_cost_fn(link, fn=routing_cost_link_cost_fn)

    def add_link_cost_fn(self, link, fn):
        """
        Add custom link cost function.

        Parameters
        ----------
        link : Link
            Network link object.
        fn : function
            The custom link cost function.
        """

        self._add_link_cost_fn(link, fn, override=True)

    def add_link_usage_fn(self, link, fn):
        """
        Add custom link usage function.

        Parameters
        ----------
        link : Link
            Network link object.
        fn : function
            The custom link usage function.
        """

        self._add_link_usage_fn(link, fn, override=True)

    def add_node_cost_fn(self, node, srv, name, fn):
        """
        Add custom node cost function.

        Parameters
        ----------
        node : Node
            Network node object.
        srv:
            Service object.
        name:
            Network service name.
        fn : function
            The custom node cost function.
        """

        self._add_node_cost_fn(node, srv, name, fn, override=True)

    def add_node_usage_fn(self, node, srv, name, fn):
        """
        Add custom node usage function.

        Parameters
        ----------
        node : Node
            Network node object.
        srv:
            Service object.
        name:
            Network service name.
        fn : function
            The custom node usage function.
        """

        self._add_node_usage_fn(node, srv, name, fn, override=True)

    def _add_link_cost_fn(self, link, fn, override=False):
        fn = default_link_cost_fn if not fn else fn
        if override:
            self.link_cost_fn_map[link.link_id] = fn
            link.add_cost_fn(fn)
        else:
            if link.link_id not in self.link_cost_fn_map:
                self.link_cost_fn_map[link.link_id] = fn
                link.add_cost_fn(fn)

    def _add_link_usage_fn(self, link, fn, override=False):
        fn = default_link_usage_fn if not fn else fn
        if override:
            self.link_usage_fn_map[link.link_id] = fn
            link.add_usage_fn(fn)
        else:
            if link.link_id not in self.link_usage_fn_map:
                self.link_usage_fn_map[link.link_id] = fn
                link.add_usage_fn(fn)

    def add_link_constraint(self, link, name, value):
        """
        Add network link constraint.

        Parameters
        ----------
        link : Link
            Network link object.
        name:
            The name of the constraint.
        value : int
            The value of the constraint.
        """

        assert isinstance(name, str) and name in ['load']
        self.link_constraint_map[link.link_id] = {'load': value}
        link.add_constraint('load', value)

    def _add_node_cost_fn(self, node, srv, name, fn, override=False):
        fn = default_node_cost_fn if not fn else fn

        def _update():
            if node.node_id not in self.node_cost_fn_map:
                self.node_cost_fn_map[node.node_id] = {}
            if srv.name not in self.node_cost_fn_map[node.node_id]:
                self.node_cost_fn_map[node.node_id][srv.name] = {}
            self.node_cost_fn_map[node.node_id][srv.name][name] = fn

        if override:
            _update()
        else:
            if node.node_id not in self.node_cost_fn_map:
                _update()
            else:
                if srv.name not in self.node_cost_fn_map[node.node_id]:
                    _update()

    def _add_node_usage_fn(self, node, srv, name, fn, override=False):
        fn = default_node_usage_fn if not fn else fn

        def _update():
            if node.node_id not in self.node_usage_fn_map:
                self.node_usage_fn_map[node.node_id] = {}

            if srv.name not in self.node_usage_fn_map[node.node_id]:
                self.node_usage_fn_map[node.node_id][srv.name] = {}
            self.node_usage_fn_map[node.node_id][srv.name][name] = fn

        if override:
            _update()
        else:
            if node.node_id not in self.node_usage_fn_map:
                _update()
            else:
                if srv.name not in self.node_usage_fn_map[node.node_id]:
                    _update()

    def add_node_constraint(self, node, srv, name, value):
        """
        Add network node constraint.

        Parameters
        ----------
        node : Node
            Network node object.
        srv : Service
            Network service function.
        name:
            The name of the constraint.
        value : int
            The value of the constraint.
        """

        if node.node_id not in self.node_constraint_map:
            self.node_constraint_map[node.node_id] = {}

        if srv.name not in self.node_constraint_map[node.node_id]:
            self.node_constraint_map[node.node_id][srv.name] = {}
        self.node_constraint_map[node.node_id][srv.name][name] = value
        node.add_constraint(srv.name, name, value)

    def add_objective(self, name):
        """
        Add network objective.

        Current objective includes 'minmaxlinkload', 'minroutingcost' and 'minmaxnodeload', 'mindelay'.

        Parameters
        ----------
        name:
            The name of the objective.
        """

        name = name.lower()
        assert isinstance(name, str) and name in ['minmaxlinkload', 'minroutingcost',
                                                  'minmaxnodeload', 'mindelay']
        if name == 'minmaxlinkload':
            self.obj_min_mlu = True
        elif name == 'minmaxnodeload':
            self.obj_min_mnu = True
        elif name == 'minroutingcost':
            self.obj_min_routing_cost = True
            if self._links:
                for link in self._links:
                    self.add_link_cost_fn(link, fn=routing_cost_link_cost_fn)
        else:
            self.obj_min_delay = True
