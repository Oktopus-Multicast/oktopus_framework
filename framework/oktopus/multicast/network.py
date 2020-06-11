from service import Service, make_service
from defaults import default_link_cost_fn, default_link_usage_fn
from defaults import default_node_cost_fn, default_node_usage_fn


class Node:
    FORMAT = '{node_id},{lat},{lon},{services_names}'
    MODEL = 'node_id:str,lat:float,lon:float,services_names:list'

    def __init__(self, node_id, lat=-1, lon=-1, services_names=None):
        self.node_id = node_id
        self.lat = lat
        self.lon = lon
        self.services = {}
        self.services_names = []

        self.constraint_map = {}
        self.cost_fn = {}
        self.usage_fn = {}

        if services_names:
            if isinstance(services_names, int):
                services_names = [services_names]
            for srv_name in services_names:
                new_srv = make_service(srv_name, ordered=True, resources_cap_dict={'cpu': 0})
                self.add_service(new_srv)

    @classmethod
    def csv_header(cls):
        return cls.FORMAT.format(node_id='node_id', lat='lat', lon='lon', services_names='services_names')

    def csv(self):
        if self.services_names:
            services_names = '"' + ','.join([str(d) for d in self.services_names]) + '"'
        else:
            services_names = '""'
        return self.FORMAT.format(node_id=self.node_id, lat=self.lat, lon=self.lon, services_names=services_names)

    def __str__(self):
        return 'Node (node_id={})'.format(self.node_id)

    def add_service(self, srv):
        assert isinstance(srv, Service)
        if srv.name not in self.services_names:
            self.services_names.append(srv.name)
        srv.node = self
        self.services[srv.name] = srv
        self.constraint_map[srv.name] = {}
        self.cost_fn[srv.name] = {}
        self.usage_fn[srv.name] = {}
        for res_name in srv.resources_cap:
            self.constraint_map[srv.name][res_name] = -1
            self.cost_fn[srv.name][res_name] = default_node_cost_fn
            self.usage_fn[srv.name][res_name] = default_node_usage_fn

    def get_services(self):
        return self.services.values()

    def get_services_map(self):
        return self.services

    def get_service(self, srv_name):
        return self.services.get(srv_name, None)

    def add_constraint(self, srv_name, res_name, value):
        self.constraint_map[srv_name][res_name] = value

    def add_cost_fn(self, fn):
        self.cost_fn = fn

    def add_usage_fn(self, fn):
        self.usage_fn = fn


class Link:
    FORMAT = '{src},{dst},{cap},{delay},{igp_weight}'
    MODEL = 'src:int,dst:int,cap:int,delay:int,igp_weight:int'

    def __init__(self, link_id, src, dst, cap, delay, bw_cost=1, igp_weight=-1, distance=-1, port1=-1, port2=-1):
        self.link_id = link_id
        self.src = src
        self.dst = dst
        self.cap = cap
        self.delay = delay
        self.used_cap = 0
        self.bw_cost = bw_cost

        self.igp_weight = igp_weight
        self.distance = distance
        self.port1 = port1
        self.port2 = port2

        self.constraint_map = {'load': -1}
        self.cost_fn = default_link_cost_fn
        self.usage_fn = default_link_usage_fn

        self.betweeness_score = 0

    @classmethod
    def csv_header(cls):
        return cls.FORMAT.format(link_id='link_id', src='src', dst='dst',
                                 cap='cap', delay='delay', igp_weight='igp_weight',
                                 distance='distance',
                                 port1='port1', port2='port2')

    def csv(self):
        return self.FORMAT.format(link_id=self.link_id, src=self.src, dst=self.dst,
                                  cap=self.cap, delay=self.delay, igp_weight=self.igp_weight,
                                  distance=self.distance,
                                  port1=self.port1, port2=self.port2)

    def __str__(self):
        return 'Link (link_id={}, src={}, dst={}, cap={}, used_cap={}, delay={}, IGP={})'.format(self.link_id, self.src,
                                                                                                 self.dst, self.cap,
                                                                                                 self.used_cap,
                                                                                                 self.delay,
                                                                                                 self.igp_weight)

    def get_load(self):
        link_cap = self.cap
        # if self.constraint_map['load'] >= 0:
        #     link_cap *= self.constraint_map['load']
        return self.used_cap / float(link_cap)

    def add_constraint(self, name, value):
        self.constraint_map[name] = value

    def add_cost_fn(self, fn):
        self.cost_fn = fn

    def add_usage_fn(self, fn):
        self.usage_fn = fn
