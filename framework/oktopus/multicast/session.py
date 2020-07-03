import collections

from network import Link

import netaddr


class Session:
    """
    Session class exposes the Session API. 
    
    The Session API allows the operator to creates sessions and adds constraints and requirements. 

    Attributes
    ----------
        addr : str
            IP address of the session.
        src : int
            Source node ID of the session.
        dsts : list or int
            Destination node ID/s of the session.
        bw : int
            Bandwidth demand constraint of the session.
        t_class : str
            Traffic class of the session.
        res : dict
            Resource consumption of the session.
        load : int
            Traffic load of the session.
        delay : int
            Traffic delay constraint for the session.
        max_hops : int
            Maximum hops constraint for the session.
        cost : int
            Routing cost of the session.
        time : int
            Runtime to allocate the session.
        num_allo : int
            Number of order when the session is allocated.
        constraint_map : dict
            Resource constraint of the session.
        avoid_map : dict
            Network element constraints. 
        pass_map : dict
            Network nodes requires to pass through.
        required_services : list
            Service chian requirement of the session.
    """

    FORMAT = '{addr},{src},{dsts},{bw},{t_class},{required_services}'
    MODEL = 'addr:str,src:int,dsts:list,bw:int,t_class:str,required_services:list'

    def __init__(self, addr, src, dsts, bw, t_class, res=None, required_services=None):
        """
        Parameters
        ----------
        addr : str
            IP address of the session.
        src : int
            Source node ID of the session.
        dsts : list or int
            Destination node ID/s of the session.
        bw : int
            Bandwidth demand constraint of the session.
        t_class : str
            Traffic class of the session.
        res : dict
            Resource consumption of the session.
        required_services : list
            Service chian requirement of the session.
        """

        self.addr = addr
        self.src = src
        self.dsts = dsts
        if isinstance(self.dsts, int):
            self.dsts = [self.dsts]
        self.bw = bw
        self.t_class = t_class

        # FIXME res[srv_name][res_name] = res_val
        self.res = res if res else {}
        self.load = -1
        self.delay = -1
        self.max_hops = -1
        self.cost = -1
        self.time = float('inf')
        self.num_allo = 0

        # user-defined constraints data structures (supported by oktopus)
        self.constraint_map = {'load': 0, 'delay': 0, 'hops': 0}
        self.avoid_map = {'nodes': [], 'links': [], 'sessions': []}
        self.pass_map = {'nodes': []}

        if isinstance(required_services, int):
            required_services = [required_services]
        self.required_services = list(required_services) if required_services else []

    @classmethod
    def csv_header(cls):
        """Get the CSV header that stores the sessions.

        Parameters
        ----------
        cls: Session
            Session object.

        Returns
        ----------
            str
                CSV header string of the session.
        """

        return cls.FORMAT.format(addr='addr', src='src', dsts='dsts', bw='bw', t_class='t_class',
                                 required_services='required_services')

    def csv(self):
        """Get the CSV format of the session.

        Returns
        ----------
            str
                CSV string of the session.
        """

        dsts = '"' + ','.join([str(d) for d in self.dsts]) + '"'
        if self.required_services:
            required_services = '"' + ','.join([str(d) for d in self.required_services]) + '"'
            print required_services
        else:
            required_services = '"\'\'"'
        return self.FORMAT.format(addr=self.addr, src=self.src, dsts=dsts, bw=self.bw, t_class=self.t_class,
                                  required_services=required_services)

    def __str__(self):
        return 'Session (addr={}, bw={}, src={}, dsts={{{}}}, max_hops={}, service chaining={})'.format(
            self.addr, self.bw, self.src, ','.join([str(d) for d in self.dsts]),
            self.max_hops,
            len(self.required_services) > 0)

    def mod_resource_req(self, srv_name, res_name, value):
        """Modify resource requirement of the session.

        Add or update the resource consumption requirement of the session.

        Parameters
        ----------
        srv_name: str
            Network service function name.
        res_name : str
            Resource name of the service function.
        value : int
            The value of the the resource.
        """

        if srv_name not in self.res:
            self.res[srv_name] = {}
        self.res[srv_name][res_name] = value

    def add_constraint(self, name, value):
        """Add constraint requirement of the session.

        Add constraint such as load, delay and hops constraints to the session requirements.

        Parameters
        ----------
        name: str
            Constraint name.
        value : int or float
            Constraint value.
        """

        assert isinstance(name, str) and name in ['load', 'delay', 'hops']
        assert (isinstance(value, float) or isinstance(value, int)) and value > 0
        self.constraint_map[name] = value

    def avoid_nodes(self, nodes):
        """Specify network nodes to avoid for the session.

        Parameters
        ----------
        nodes: list
            List of network nodes.
        """

        assert isinstance(nodes, collections.Iterable) and len(nodes) > 0
        for n in nodes:
            value = -1
            if isinstance(n, int):
                value = n
            elif isinstance(n, str):
                try:
                    value = int(n)
                except ValueError as ex:
                    print ex

            if value > -1:
                self.avoid_map['nodes'].append(n)

    def avoid_links(self, links):
        """Specify network links to avoid for the session.

        Parameters
        ----------
        links: list
            List of network links.
        """

        assert isinstance(links, collections.Iterable) and len(links) > 0
        for l in links:
            link_tuple = None
            if isinstance(l, str):
                try:
                    trimmed = l.strip().replace(' ', '')
                    trimmed = trimmed.replace('(', '').replace(')', '')
                    if ',' in trimmed:
                        splitted = trimmed.split(',')
                        if len(splitted) == 2:
                            src = int(splitted[0])
                            dst = int(splitted[1])
                            link_tuple = (src, dst)
                except ValueError as ex:
                    print ex

            elif isinstance(l, Link):
                link_tuple = (l.src, l.dst)

            if link_tuple:
                self.avoid_map['links'].append(link_tuple)

    def avoid_sessions(self, sessions):
        """Specify session to avoid for the session.

        Parameters
        ----------
        sessions: list
            List of network sessions.
        """

        assert isinstance(sessions, collections.Iterable) and len(sessions) > 0
        for s in sessions:
            addr = None
            if isinstance(s, Session):
                addr = s.addr
            elif isinstance(s, str):
                try:
                    ip = netaddr.IPAddress(s)
                    addr = ip.ipv4()
                except netaddr.core.AddrFormatError as ex:
                    print ex

            if addr:
                self.avoid_map['sessions'].append(addr)

    def pass_through(self, nodes):
        """Specify network nodes requires to pass through for the session.

        Parameters
        ----------
        nodes: list
            List of network nodes.
        """

        assert isinstance(nodes, collections.Iterable) and len(nodes) > 0
        new_nodes = []
        for n in nodes:
            if isinstance(n, int):
                new_nodes.append(n)
            elif isinstance(n, str):
                try:
                    new_nodes.append(int(n))
                except ValueError as ex:
                    new_nodes.append(n)

        if new_nodes:
            self.pass_map['nodes'].append(new_nodes)

    def traverse(self, services):
        """Specify the service chain requirement of the session.

        Parameters
        ----------
        services: list
            A order list of service function.
        """

        assert isinstance(services, collections.Iterable) and len(services) > 0
        for srv in services:
            # if (isinstance(srv, str) or isinstance(srv, int)) and srv not in self.required_services:
            if (isinstance(srv, str) or isinstance(srv, int)):
                self.required_services.append(srv)

    def print_constraints(self):
        """Print the constraints and requirements of the session."""

        if self.constraint_map['hops'] > 0:
            print 'Max. hops constraint:', self.constraint_map['hops']
        if self.constraint_map['delay'] > 0:
            print 'Delay constraint:', self.constraint_map['delay']
        if self.avoid_map['nodes']:
            print 'Avoid nodes:', self.avoid_map['nodes']
        if self.avoid_map['links']:
            print 'Avoid links:', self.avoid_map['links']
        if self.avoid_map['sessions']:
            print 'Avoid sessions:'
            for s in self.avoid_map['sessions']:
                print '\t', s
        if self.pass_map['nodes']:
            print 'Pass nodes:'
            for path_idx, nodes in enumerate(self.pass_map['nodes'], start=1):
                print '\t{}- {}'.format(path_idx, ' -> '.join(str(n) for n in nodes))

        if self.required_services:
            print 'Services:'
            for srv_idx, srv_name in enumerate(self.required_services, start=1):
                print '{}- {}:'.format(srv_idx, srv_name)
                if srv_name in self.res:
                    for res_name, res_value in self.res[srv_name].iteritems():
                        print '\t', res_name, res_value
