import os
import sys

from oktopus import load_sessions as ok_load_sessions
from oktopus import App, Routing, make_service
from oktopus.dataset import OK_SESSIONS_FILE
from oktopus.multicast.defaults import routing_cost_node_cost_fn
from memory_profiler import memory_usage

TCAM_CAP = 1000000


def main(topo_name, topo_path, in_dir, session_count, algorithm_name, objective='routing'):
    # Load sessions
    sessions_file = os.path.join(in_dir, OK_SESSIONS_FILE)
    sessions = ok_load_sessions(sessions_file % session_count)

    # Create a new Oktopus Application
    app = App(topo_name, topo_path)

    # Load sessions to Oktopus App
    app.add_sessions(sessions)

    # Create services
    for node in app.get_nodes():
        sdn_router = make_service('sdn_router', ordered=False, resources_cap_dict={'tcam': TCAM_CAP})
        node.add_service(sdn_router)
    
    for session in app.get_sessions():
        session.mod_resource_req('sdn_router', 'tcam', 1)

    routing = Routing()
    if objective == 'routing':
        routing.add_objective('minRoutingCost')
    elif objective == 'link_load':
        routing.add_objective('minMaxLinkLoad')
    elif objective == 'node_load':
        routing.add_objective('minMaxNodeLoad')
    elif objective == 'delay':
        routing.add_objective('minDelay')
    else:
        print 'Unsupported objective function'
        exit(0)

    for node in app.get_nodes():
        sdn_router = node.get_service('sdn_router')
        routing.add_node_constraint(node, sdn_router, 'tcam', TCAM_CAP)
        routing.add_node_cost_fn(node, sdn_router, 'tcam', routing_cost_node_cost_fn)

    for link in app.get_links():
        routing.add_link_constraint(link, 'load', 1.)

    app.set_routes(routing)
    if algorithm_name == 'oktopus':
        app.solve(algorithm='oktopus', ppp=2000, pool_size=4)
    elif algorithm_name == 'mtrsa':
        app.solve(algorithm='mtrsa')
    elif algorithm_name == 'mldp':
        app.solve(algorithm='mldp', metric='delay')
    elif algorithm_name == 'cplex_mte':
        app.solve(algorithm='cplex_mte', time_limit=60*60*12, num_worker=5)
    else:
        print 'Unsupported algorithm'
        exit(0)


if __name__ == '__main__':
    if len(sys.argv) == 6:
        topo_name = sys.argv[1]
        graph_path = sys.argv[2]
        input_dir = sys.argv[3]
        sessions_count = int(sys.argv[4])
        algorithm_name = sys.argv[5].lower()
        main(topo_name, graph_path, input_dir, sessions_count, algorithm_name)
    elif len(sys.argv) == 7:
        topo_name = sys.argv[1]
        graph_path = sys.argv[2]
        input_dir = sys.argv[3]
        sessions_count = int(sys.argv[4])
        algorithm_name = sys.argv[5].lower()
        objective = sys.argv[6].lower()
        # main(topo_name, graph_path, input_dir, sessions_count, algorithm_name, objective)
        print "Peak Memory: {} MB".format(max(memory_usage((main, ((topo_name, graph_path, input_dir, sessions_count, algorithm_name, objective))))))
    else:
        print 'python min_max_link_util.py <topo_name> <graph_path> <input_dir> <session_count> <alg_name> [<objective>]'
        print '    alg_name : mtrsa or oktopus'
        print '    objective: routing or delay or link_load'
