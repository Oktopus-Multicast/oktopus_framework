import os
from random import uniform
from netaddr import IPNetwork, IPAddress

from networkx import read_graphml
import networkx


def ensure_dir(directory):
    if not os.path.isdir(directory) or not os.path.exists(directory):
        os.makedirs(directory)


def read_isp_graph(file_path):
    graph = None
    try:
        graph = read_graphml(file_path, node_type=int)
        graph.remove_nodes_from(list(networkx.isolates(graph)))
    except Exception as ex:
        print ex
    return graph


def parse_capacities(link_capacity):
    try:
        l_cap = float(link_capacity)
        l_cap *= 1000000.
        return l_cap
    except Exception as ex:
        print ex
    return None


def get_resources_file(file_path, o_dir):
    file_name, file_extension = os.path.splitext(file_path)
    if '/' in file_name:
        splitted_file_name = file_name.split('/')
        file_name = splitted_file_name[-1]
    return os.path.join(o_dir, file_name + '_resources' + file_extension)


def random_pick(sorted_list):
    x = uniform(0, 1)
    cumulative_probability = 0.0
    selected_item = None
    for item in sorted_list:
        cumulative_probability += item[1]
        if x < cumulative_probability:
            selected_item = item[0]
            break
    return selected_item


def get_ip(i, base):
    assert isinstance(i, int) and i > 0
    ip_base = IPNetwork(base)
    return str(IPAddress(int(ip_base.ip) + i))