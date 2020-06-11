import os
import sys

from oktopus import load_sessions as ok_load_sessions
from oktopus import App, Routing, make_service
from oktopus.dataset import OK_NODES_SERVICES_FILE, OK_SESSIONS_SERVICES_FILE
from memory_profiler import memory_usage

SRV_CAP = 500
SRV_USAGE = 1

def main(topo_name, topo_path, in_dir, session_count, algorithm_name, objective='routing'):
    SRV_CAP = session_count
    # Load sessions
    sessions_file = os.path.join(in_dir, OK_SESSIONS_SERVICES_FILE)
    sessions = ok_load_sessions(sessions_file % session_count)

    # Create a new Oktopus Application
    node_file = os.path.join(in_dir, OK_NODES_SERVICES_FILE)
    app = App(topo_name, topo_path, node_file=node_file, parse_services=True)

    # Load sessions to Oktopus App
    app.add_sessions(sessions)

    # Create services
    for node in app.get_nodes():
        sdn_router = make_service('sdn_router', ordered=False, resources_cap_dict={'tcam': 10000})
        node.add_service(sdn_router)
        node_services = node.get_services()
        for srv in node_services:
            if srv.ordered:
                srv.set_available_cap('cpu', SRV_CAP)

    # Set session requirements
    for session in app.get_sessions():
        if session.required_services:
            for srv_name in session.required_services:
                session.mod_resource_req(srv_name, 'cpu', SRV_USAGE)
            # session.required_services = []
        session.mod_resource_req('sdn_router', 'tcam', 1)

    routing = Routing()

    if objective == 'routing':
        routing.add_objective('minRoutingCost')
    elif objective == 'link_load':
        routing.add_objective('minMaxLinkLoad')
    elif objective == 'delay':
        routing.add_objective('minDelay')
    else:
        print 'Unsupported objective function'
        exit(0)

    for node in app.get_nodes():
        for srv in node.get_services():
            if srv.ordered:
                routing.add_node_constraint(node, srv, 'cpu', SRV_CAP)

    for link in app.get_links():
        routing.add_link_constraint(link, 'load', 1.)

    app.set_routes(routing)
    if algorithm_name == 'oktopus':
        app.solve(algorithm='oktopus', ppp=2000, pool_size=4)
    elif algorithm_name == 'msa':
        app.solve(algorithm='msa')
    elif algorithm_name == 'cplex_sc':
        app.solve(algorithm='cplex_sc', time_limit=60*60*3, num_worker=5)
    else:
        print 'Unsupported algorithm'
        exit(0)

    # for node in app.get_nodes():
    #     print node.node_id
    #     for srv in node.get_services():
    #         if srv.ordered:
    #             print srv.name, srv.get_available_cap('cpu')


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
        print 'python min_mlu_service_chaining.py <topo_name> <graph_path> <input_dir> <session_count> <alg_name> [<objective>]'
        print '    alg_name                   : msa or oktopus'
        print '    objective                  : routing or delay or link_load'

