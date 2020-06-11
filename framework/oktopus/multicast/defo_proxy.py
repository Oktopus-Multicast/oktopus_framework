import re
import networkx as nx

from .application import App
from .session import Session


def _parse_topology(topology_file):
    graph = nx.DiGraph()
    with open(topology_file, 'r') as tf:
        lines = [line.strip() for line in tf]

    parse_nodes = False
    parse_links = False
    skip_header = True
    relabel_nodes = False
    # node_idx => node_name
    node_idx_map = {}
    orig_ok_node_map = {}
    node_idx = 0

    node_pattern = '^N[0-9]+$'
    node_re = re.compile(node_pattern)

    # parse nodes
    for line in lines:
        if line.lower() == 'nodes':
            parse_nodes = True
            continue
        elif not line:
            parse_nodes = False
            skip_header = True
            continue

        if parse_nodes and skip_header:
            skip_header = False
            continue

        if parse_nodes:
            splitted = line.split(' ')
            if splitted and len(splitted) == 3:
                orig_node_name = splitted[0]
                node_name = splitted[0]
                match = node_re.match(node_name)
                if match:
                    ok_node_name = int(match.group(0)[1:])
                    if ok_node_name == 0:
                        relabel_nodes = True
                else:
                    ok_node_name = node_idx + 1
                lat = float(splitted[1])
                lon = float(splitted[2])
                node_idx_map[node_idx] = ok_node_name
                orig_ok_node_map[orig_node_name] = ok_node_name
                node_idx += 1
            else:
                print 'X invalid node line'
    if relabel_nodes:
        node_idx_map = {k: v + 1 for k, v in node_idx_map.iteritems()}
        orig_ok_node_map = {k: v + 1 for k, v in orig_ok_node_map.iteritems()}

    for k, v in node_idx_map.iteritems():
        graph.add_node(int(v), attr_dict={'Latitude': 0, 'Longitude': 0})

    # parse links
    skip_header = True
    for line in lines:
        if line.lower() == 'edges':
            parse_links = True
            skip_header = True
            continue
        elif not line:
            parse_links = False
            skip_header = True
            continue

        if parse_links and skip_header:
            skip_header = False
            continue

        if parse_links:
            splitted = line.split(' ')
            if splitted and len(splitted) == 6:
                src = int(splitted[1])
                dst = int(splitted[2])
                weight = int(splitted[3])
                cap = int(splitted[4])
                delay = int(splitted[5])
                src = node_idx_map[src]
                dst = node_idx_map[dst]

                graph.add_edge(int(src), int(dst), attr_dict={'Capacity': cap,
                                                              'IGPWeight': weight,
                                                              'Distance': 0.,
                                                              'Delay': delay,
                                                              'src_port': -1,
                                                              'dst_port': -1})
            else:
                print 'X invalid link line'
    return graph, node_idx_map, orig_ok_node_map, relabel_nodes


def _parse_demands(demands_file, node_idx_map, relabel_nodes):
    sessions = []
    with open(demands_file, 'r') as tf:
        lines = [line.strip() for line in tf]

    parse_demands = False
    skip_header = True

    # parse nodes
    for line in lines:
        if line.lower() == 'demands':
            parse_demands = True
            continue
        elif not line:
            parse_demands = False
            skip_header = True
            continue

        if parse_demands and skip_header:
            skip_header = False
            continue

        if parse_demands:
            splitted = line.split(' ')
            if splitted and len(splitted) == 4:
                addr = splitted[0]
                src = int(splitted[1])
                dst = int(splitted[2])
                # if relabel_nodes:
                #     src += 1
                #     dst += 1
                bw = int(splitted[3])
                if src != dst:
                    new_session = Session(addr=addr, src=node_idx_map[src],
                                          dsts=[node_idx_map[dst]], bw=bw, t_class='None')
                    sessions.append(new_session)
    return sessions


def from_defo(topo_name, topology_file, demands_file, constraints_file, technology='sr'):
    graph, node_idx_map, orig_ok_node_map, relabel_nodes = _parse_topology(topology_file)
    app = App(topo_name, graph, technology)
    if demands_file:
        sessions = _parse_demands(demands_file, node_idx_map, relabel_nodes)
        app.add_sessions(sessions)
    return app


def to_defo(oktopus_app):
    pass
