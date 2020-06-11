# FIXME consider modify the link.cap given the constraint


def default_link_cost_fn(link, session):
    link_cap = link.cap
    if link.constraint_map['load'] >= 0:
        link_cap *= link.constraint_map['load']
    val = (session.bw + link.used_cap) / float(link_cap)
    if val < 0.3:
        return val
    elif val < 0.6:
        return 4 * val
    elif val < 0.9:
        return 8 * val
    elif val < 1:
        return 16 * val
    return 32 * val


def routing_cost_link_cost_fn(link, session):
    if link.used_cap + session.bw > link.cap:
        return float('inf')
        
    # used_cap_multiplier = 1 if link.used_cap == 0 else link.used_cap
    # # val = link.bw_cost * session.bw  * used_cap_multiplier**3
    # val = link.bw_cost * session.bw  + (1.0 - 1/used_cap_multiplier) + link.betweeness_score*100*session.bw 
    # val = link.bw_cost * session.bw 

    used = (link.used_cap+session.bw ) / float(link.cap)

    # import math
    r = 2
    limit = 1 / float(r)
    if used >= limit:
        # val = - 1 / float(r*math.log(used-0.000000001, r+r*link.betweeness_score)) 
        val = (used*100)**(2+link.betweeness_score*2)
    else:
        val = session.bw / float(link.cap)
 
    # print "val {}, between {}, use {}".format(val, betweeen, used)
    # print "used {}, 1-link.betweeness_score {}".format(used, 1-link.betweeness_score)
    return val
    if val < 0.3:
        return val
    elif val < 0.6:
        return 4 * val
    elif val < 0.9:
        return 8 * val
    elif val < 1:
        return 16 * val
    return 32 * val


def routing_cost_node_cost_fn(node, session, srv_name, res_name):
    srv = node.services[srv_name]
    if srv.get_available_cap(srv_name) > 1:
        return 0
    return 1.

    # srv = node.services[srv_name]
    # srv_cap = srv.resources_cap[res_name]
    # if srv_name in node.constraint_map and res_name in node.constraint_map[srv_name]:
    #     if node.constraint_map[srv_name][res_name] >= 0:
    #         srv_cap = node.constraint_map[srv.name][res_name]
    # session_res = session.res[srv_name][res_name]
    # used_cap = srv.used_cap[res_name]
    # return (session_res + used_cap) / float(srv_cap)


def default_link_usage_fn(link, session):
    link.used_cap += session.bw


def default_node_cost_fn(node, session, srv_name, res_name):
    srv = node.services[srv_name]
    srv_cap = srv.resources_cap[res_name]
    if srv_name in node.constraint_map and res_name in node.constraint_map[srv_name]:
        if node.constraint_map[srv_name][res_name] >= 0:
            srv_cap = node.constraint_map[srv.name][res_name]
    session_res = session.res[srv_name][res_name]
    used_cap = srv.used_cap[res_name]
    val = (session_res + used_cap) / float(srv_cap)
    if val < 0.3:
        return val
    elif val < 0.6:
        return 4 * val
    elif val < 0.9:
        return 8 * val
    elif val < 1:
        return 16 * val
    return 32 * val


def default_node_usage_fn(node, session, srv_name, res_name):
    srv = node.services[srv_name]
    session_res = session.res[srv.name][res_name]
    srv.used_cap[res_name] += session_res

