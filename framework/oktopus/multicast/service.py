

class Service:
    def __init__(self, name, resources_cap, ordered):
        assert isinstance(resources_cap, dict)
        self.name = name
        self.ordered = ordered
        self.resources_cap = resources_cap
        self.used_cap = {resource_name: 0 for resource_name in resources_cap}
        self.node = None

    def __str__(self):
        resources_str = ','.join(['({}:{})'.format(res_name, res_val)
                                  for res_name, res_val in self.resources_cap.iteritems()])
        return 'Service (name={}, ordered={}, resources={})'.format(self.name, self.ordered, resources_str)

    def get_available_cap(self, res_name):
        if res_name in self.resources_cap:
            return self.resources_cap[res_name] - self.used_cap[res_name]

    def set_available_cap(self, res_name, res_value):
        if res_name in self.resources_cap:
            self.resources_cap[res_name] = res_value


def make_service(name, resources_cap_dict, ordered):
    return Service(name, resources_cap_dict, ordered)
