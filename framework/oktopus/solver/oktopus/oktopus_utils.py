"""
oktopus algorithm related utils
"""
import random, statistics
from multiprocessing import Pool
from collections import defaultdict, deque
from sys import maxint

import networkx as nx
from cytoolz import merge, partial
from nx_disjoint_paths import edge_disjoint_paths


from ...multicast.session import Session


def pick_dst(session, priority_list, ignore=None):
    assert isinstance(session, Session)
    candidate_set = set(session.dsts)
    if ignore:
        ignore_set = set(ignore)
        candidate_set = candidate_set.difference(ignore_set)
    if not candidate_set:
        return None

    if priority_list:
        for item in priority_list:
            if item in candidate_set:
                return item

    items = [(dst, 1. / float(len(candidate_set))) for dst in candidate_set]
    x = random.uniform(0, 1)
    cumulative_probability = 0.0
    dst = None
    for item in items:
        cumulative_probability += item[1]
        if x < cumulative_probability:
            dst = item[0]
            break
    return dst


def construct_links_from_path(path, links_map):
    i = 0
    j = 1
    links = []
    while j < len(path):
        link_key = (int(path[i]), int(path[j]))
        links.append(links_map[link_key])
        i += 1
        j += 1
    return links


def _all_simple_paths_dfs(graph, links_map, ppp, node_pair):
    source = node_pair[0]
    target = node_pair[1]
    cutoff = calculate_cutoff(graph, source, target, k=2)
    paths = []
    paths_nodes = []
    paths_links_ids = []
    visited = [source]
    stack = [iter(graph[source])]
    if cutoff < 4:
        cutoff *= 2
    while stack:
        children = stack[-1]
        child = next(children, None)
        if child is None:
            stack.pop()
            visited.pop()
        elif len(visited) < cutoff:
            if child == target:
                new_path = visited + [target]
                int_path = [int(n) for n in new_path]
                links = construct_links_from_path(new_path, links_map)
                links_ids = [(l.src, l.dst) for l in links]
                paths.append(links)
                paths_nodes.append(int_path)
                paths_links_ids.append(links_ids)
            elif child not in visited:
                visited.append(child)
                stack.append(iter(graph[child]))
        else:  # len(visited) == cutoff:
            if child == target or target in children:
                new_path = visited + [target]
                int_path = [int(n) for n in new_path]
                links = construct_links_from_path(new_path, links_map)
                links_ids = [(l.src, l.dst) for l in links]
                paths.append(links)
                paths_nodes.append(int_path)
                paths_links_ids.append(links_ids)
            stack.pop()
            visited.pop()
        if len(paths) == ppp:
            break
    # paths = sorted(paths, key=lambda path: len(path))
    key_pair = (int(source), int(target))
    return {key_pair: (paths, paths_nodes, paths_links_ids)}
    # return {key_pair: (paths, paths_nodes, paths_links_ids,  len(list(nx.all_simple_paths(graph, source=key_pair[0], target=key_pair[1]))))}

def _all_simple_paths_disjoint(graph, links_map, ppp, node_pair):
    source = node_pair[0]
    target = node_pair[1]

    paths = []
    paths_nodes = []
    paths_links_ids = []

    for p in edge_disjoint_paths(graph, source, target):
        int_path = [int(n) for n in p]
        links = construct_links_from_path(p, links_map)
        links_ids = [(l.src, l.dst) for l in links]
        paths.append(links)
        paths_nodes.append(int_path)
        paths_links_ids.append(links_ids)

    key_pair = (int(source), int(target))
    return {key_pair: (paths, paths_nodes, paths_links_ids)}

def _all_simple_paths_dfs_disjoint(graph, links_map, ppp, node_pair):
    source = node_pair[0]
    target = node_pair[1]
    cutoff = calculate_cutoff(graph, source, target, k=2)
    paths = []
    paths_nodes = []
    paths_links_ids = []
    visited = [source]
    stack = [iter(graph[source])]
    # print ">>>>>>>>>>>>>>>>>>", source, " ", target, ":::::", cutoff
    if cutoff < 4:
        cutoff *= 2
    while stack:
        children = stack[-1]
        child = next(children, None)
        if child is None:
            stack.pop()
            visited.pop()
        elif len(visited) < cutoff:
            if child == target:
                new_path = visited + [target]
                int_path = [int(n) for n in new_path]
                links = construct_links_from_path(new_path, links_map)
                links_ids = [(l.src, l.dst) for l in links]
                paths.append(links)
                paths_nodes.append(int_path)
                paths_links_ids.append(links_ids)
            elif child not in visited:
                visited.append(child)
                stack.append(iter(graph[child]))
        else:  # len(visited) == cutoff:
            if child == target or target in children:
                new_path = visited + [target]
                int_path = [int(n) for n in new_path]
                links = construct_links_from_path(new_path, links_map)
                links_ids = [(l.src, l.dst) for l in links]
                paths.append(links)
                paths_nodes.append(int_path)
                paths_links_ids.append(links_ids)
            stack.pop()
            visited.pop()
        if len(paths) == ppp:
            break

    for p in edge_disjoint_paths(graph, source, target):
        # print "DISJOINT from {} to {}: {}".format(source, target, p)
        int_path = [int(n) for n in p]
        links = construct_links_from_path(p, links_map)
        links_ids = [(l.src, l.dst) for l in links]
        paths.append(links)
        paths_nodes.append(int_path)
        paths_links_ids.append(links_ids)

    # paths = sorted(paths, key=lambda path: len(path))
    key_pair = (int(source), int(target))
    return {key_pair: (paths, paths_nodes, paths_links_ids)}
    # return {key_pair: (paths, paths_nodes, paths_links_ids,  len(list(nx.all_simple_paths(graph, source=key_pair[0], target=key_pair[1]))))}



def _all_simple_paths_bfs_dfs_disjoint(graph, links_map, ppp, node_pair):
    source = node_pair[0]
    target = node_pair[1]

    paths = []
    paths_nodes = []
    paths_links_ids = []

    queue = [(source, [source])]

    while queue:
        current, path = queue.pop(0)

        if path[-1] == target:
            int_path = [int(n) for n in path]
            links = construct_links_from_path(path, links_map)
            links_ids = [(l.src, l.dst) for l in links]
            paths.append(links)
            paths_nodes.append(int_path)
            paths_links_ids.append(links_ids)
        
        for neighbor in graph[current]:
            # if neighbor not in path and len(path) < cutoff:
            if neighbor not in path:
                queue.append((neighbor, path + [neighbor]))

        if len(paths) == ppp:
            break


    cutoff = calculate_cutoff(graph, source, target, k=2)
    visited = [source]
    stack = [iter(graph[source])]

    if cutoff < 4:
        cutoff *= 2
    while stack:
        children = stack[-1]
        child = next(children, None)
        if child is None:
            stack.pop()
            visited.pop()
        elif len(visited) < cutoff:
            if child == target:
                new_path = visited + [target]
                int_path = [int(n) for n in new_path]
                links = construct_links_from_path(new_path, links_map)
                links_ids = [(l.src, l.dst) for l in links]
                paths.append(links)
                paths_nodes.append(int_path)
                paths_links_ids.append(links_ids)
            elif child not in visited:
                visited.append(child)
                stack.append(iter(graph[child]))
        else:  # len(visited) == cutoff:
            if child == target or target in children:
                new_path = visited + [target]
                int_path = [int(n) for n in new_path]
                links = construct_links_from_path(new_path, links_map)
                links_ids = [(l.src, l.dst) for l in links]
                paths.append(links)
                paths_nodes.append(int_path)
                paths_links_ids.append(links_ids)
            stack.pop()
            visited.pop()
        if len(paths) == ppp:
            break

    for p in edge_disjoint_paths(graph, source, target):
        # print "DISJOINT from {} to {}: {}".format(source, target, p)
        int_path = [int(n) for n in p]
        links = construct_links_from_path(p, links_map)
        links_ids = [(l.src, l.dst) for l in links]
        paths.append(links)
        paths_nodes.append(int_path)
        paths_links_ids.append(links_ids)

    # paths = sorted(paths, key=lambda path: len(path))
    key_pair = (int(source), int(target))
    return {key_pair: (paths, paths_nodes, paths_links_ids)}
    # return {key_pair: (paths, paths_nodes, paths_links_ids,  len(list(nx.all_simple_paths(graph, source=key_pair[0], target=key_pair[1]))))}


def _all_simple_paths_bfs_disjoint(graph, links_map, ppp, node_pair):
    source = node_pair[0]
    target = node_pair[1]

    paths = []
    paths_nodes = []
    paths_links_ids = []

    cutoff = calculate_cutoff(graph, source, target, k=2)
    queue = [(source, [source])]

    cutoff= nx.diameter(graph)

    while queue:
        current, path = queue.pop(0)

        if path[-1] == target:
            int_path = [int(n) for n in path]
            links = construct_links_from_path(path, links_map)
            links_ids = [(l.src, l.dst) for l in links]
            paths.append(links)
            paths_nodes.append(int_path)
            paths_links_ids.append(links_ids)
            continue
        
        for neighbor in graph[current]:
            if neighbor not in path and len(path) < cutoff:
                queue.append((neighbor, path + [neighbor]))

        if len(paths) == ppp:
            break

    try:
        for p in edge_disjoint_paths(graph, source, target):
            int_path = [int(n) for n in p]
            if int_path in paths_nodes:
                continue
            links = construct_links_from_path(p, links_map)
            links_ids = [(l.src, l.dst) for l in links]
            paths.append(links)
            paths_nodes.append(int_path)
            paths_links_ids.append(links_ids)
    except:
        print "No path", source, target

    # paths = sorted(paths, key=lambda path: len(path))
    key_pair = (int(source), int(target))
    return {key_pair: (paths, paths_nodes, paths_links_ids)}
    # return {key_pair: (paths, paths_nodes, paths_links_ids,  len(list(nx.all_simple_paths(graph, source=key_pair[0], target=key_pair[1]))))}

############################
#  Graph contraction
############################

def contract_linear_links(graph):
    def find(data, i):
        if i != data[i]:
            data[i] = find(data, data[i])
        return data[i]

    def union(data, i, j):
        pi, pj = find(data, i), find(data, j)
        if pi != pj:
            data[pj] = pi

    supernodes = dict()
    supernodes_paths = defaultdict(dict)

    for n in graph.nodes():
        supernodes[n] = n

    def edge_contract(graph, node, original_graph):
        new_graph = graph

        queue = deque()
        visited = set()

        if len(original_graph.edges(node)) <= 2 and any([len(original_graph.edges(n)) <= 2 for _, n in original_graph.edges(node)]): 
            queue.append(node)

        while len(queue) > 0:
            curr_node = queue.popleft()
            if curr_node not in visited:
                visited.add(curr_node)
                for head_node, tail_node in original_graph.edges(curr_node):
                    if len(original_graph.edges(tail_node)) <= 2:
                        if tail_node in visited:
                            continue

                        queue.append(tail_node)
                        new_graph = nx.contracted_edge(new_graph, (node, tail_node), False)
                        union(supernodes, node, tail_node)

                        if "contracted_edges" not in supernodes_paths[node]:
                            supernodes_paths[node]["contracted_edges"] = [head_node, tail_node]
                            supernodes_paths[node]["every_pair"] = dict()
                            supernodes_paths[node]["every_pair"][(head_node, tail_node)] = [head_node, tail_node]
                            supernodes_paths[node]["every_pair"][(tail_node, head_node)] = [tail_node, head_node]

                        elif supernodes_paths[node]["contracted_edges"][0] == head_node:
                            supernodes_paths[node]["contracted_edges"].insert(0, tail_node)
                            for i in range(1, len(supernodes_paths[node]["contracted_edges"])):
                                supernodes_paths[node]["every_pair"][(supernodes_paths[node]["contracted_edges"][0], supernodes_paths[node]["contracted_edges"][i])] = supernodes_paths[node]["contracted_edges"][:i+1]
                                supernodes_paths[node]["every_pair"][(supernodes_paths[node]["contracted_edges"][i], supernodes_paths[node]["contracted_edges"][0])] = supernodes_paths[node]["contracted_edges"][:i+1][::-1]

                        elif supernodes_paths[node]["contracted_edges"][-1] == head_node:
                            supernodes_paths[node]["contracted_edges"].append(tail_node)
                            for i in range(len(supernodes_paths[node]["contracted_edges"])-1):
                                supernodes_paths[node]["every_pair"][(supernodes_paths[node]["contracted_edges"][i], supernodes_paths[node]["contracted_edges"][-1])] = supernodes_paths[node]["contracted_edges"][i:]
                                supernodes_paths[node]["every_pair"][(supernodes_paths[node]["contracted_edges"][-1], supernodes_paths[node]["contracted_edges"][i])] = supernodes_paths[node]["contracted_edges"][i:][::-1]

                    else:
                        if "supernode_edges" not in supernodes_paths[node]:
                            supernodes_paths[node]["supernode_edges"] = dict()
                        supernodes_paths[node]["supernode_edges"][tail_node] = head_node
                    
        return new_graph

    shrink_graph = graph
    for n in graph.nodes():
        if n in shrink_graph.nodes():
            shrink_graph = edge_contract(shrink_graph, n, graph)

    supernode_data = {'find_struc': supernodes, 'paths': supernodes_paths}

    return (supernode_data, shrink_graph)

############################
#  End - Graph contraction
############################



def _all_simple_paths_dfs_super(shrink_graph, graph, supernodes_data, links_map, ppp, node_pair):

    supernodes = supernodes_data['find_struc']
    supernodes_paths = supernodes_data['paths']

    def find(data, i):
        if i != data[i]:
            data[i] = find(data, data[i])
        return data[i]


    def translate_path(path_from_shrink):
        res = []

        if path_from_shrink[0] not in supernodes_paths:
            res.append(node_pair[0])
        else:
            last_node = supernodes_paths[path_from_shrink[0]]["supernode_edges"][path_from_shrink[1]]
            if last_node == node_pair[0]:
                res.append(node_pair[0])
            else:
                res += supernodes_paths[path_from_shrink[0]]["every_pair"][(node_pair[0], last_node)]


        for i in range(1, len(path_from_shrink)-1):
            if path_from_shrink[i] in supernodes_paths:
                if supernodes_paths[path_from_shrink[i]]["supernode_edges"][res[-1]] == supernodes_paths[path_from_shrink[i]]["contracted_edges"][0]:
                    res += supernodes_paths[path_from_shrink[i]]["contracted_edges"]
                else:
                    res += supernodes_paths[path_from_shrink[i]]["contracted_edges"][::-1]
            else:
                res.append(path_from_shrink[i])

        if path_from_shrink[-1] not in supernodes_paths:
            res.append(node_pair[1])
        else:
            last_node = supernodes_paths[path_from_shrink[-1]]["supernode_edges"][res[-1]]
            if last_node == node_pair[1]:
                res.append(node_pair[1])
            else:
                res += supernodes_paths[path_from_shrink[-1]]["every_pair"][(last_node, node_pair[1])]

        return res

    source = node_pair[0]
    target = node_pair[1]
    source = find(supernodes, node_pair[0])
    target = find(supernodes, node_pair[1])

    cutoff = calculate_cutoff(shrink_graph, source, target, k=2)
    paths = []
    paths_nodes = []
    paths_links_ids = []
    visited = [source]
    stack = [iter(shrink_graph[source])]
    while stack:
        children = stack[-1]
        child = next(children, None)
        if child is None:
            stack.pop()
            visited.pop()
        elif len(visited) < cutoff:

            if child == target:
                new_path = visited + [target]

                new_path = translate_path(new_path)

                int_path = [int(n) for n in new_path]
                links = construct_links_from_path(new_path, links_map)
                links_ids = [(l.src, l.dst) for l in links]
                paths.append(links)
                paths_nodes.append(int_path)
                paths_links_ids.append(links_ids)
            elif child not in visited:
                visited.append(child)
                stack.append(iter(shrink_graph[child]))
        else:  # len(visited) == cutoff:
            if child == target or target in children:
                new_path = visited + [target]

                new_path = translate_path(new_path)

                int_path = [int(n) for n in new_path]
                links = construct_links_from_path(new_path, links_map)
                links_ids = [(l.src, l.dst) for l in links]
                paths.append(links)
                paths_nodes.append(int_path)
                paths_links_ids.append(links_ids)
            stack.pop()
            visited.pop()
        if len(paths) == ppp:
            break

    if source == target and node_pair[0] != node_pair[1]:
        new_path = supernodes_paths[source]["every_pair"][(node_pair[0], node_pair[1])]

        int_path = [int(n) for n in new_path]
        links = construct_links_from_path(new_path, links_map)
        links_ids = [(l.src, l.dst) for l in links]
        paths.append(links)
        paths_nodes.append(int_path)
        paths_links_ids.append(links_ids)

    key_pair = (int(node_pair[0]), int(node_pair[1]))

    return {key_pair: (paths, paths_nodes, paths_links_ids)}


def _all_simple_paths(graph, links_map, ppp, node_pair):
    source = node_pair[0]
    target = node_pair[1]

    paths = []
    paths_nodes = []
    paths_links_ids = []
    count  =0 
    for path in nx.all_simple_paths(graph, source=source, target=target):
        if  count >= ppp:
            break
        int_path = [int(n) for n in path]
        links = construct_links_from_path(path, links_map)
        links_ids = [(l.src, l.dst) for l in links]
        paths.append(links)
        paths_nodes.append(int_path)
        paths_links_ids.append(links_ids)
        count += 1

    key_pair = (int(source), int(target))
    return {key_pair: (paths, paths_nodes, paths_links_ids)}


def _all_simple_paths_bfs(graph, links_map, ppp, node_pair):
    source = node_pair[0]
    target = node_pair[1]

    paths = []
    paths_nodes = []
    paths_links_ids = []

    cutoff = calculate_cutoff(graph, source, target, k=2)
    queue = [(source, [source])]

    if cutoff < 4:
        cutoff *= 2


    while queue:
        current, path = queue.pop(0)

        if path[-1] == target:
            int_path = [int(n) for n in path]
            links = construct_links_from_path(path, links_map)
            links_ids = [(l.src, l.dst) for l in links]
            paths.append(links)
            paths_nodes.append(int_path)
            paths_links_ids.append(links_ids)
        
        for neighbor in graph[current]:
            if neighbor not in path and len(path) < cutoff:
                queue.append((neighbor, path + [neighbor]))

        if len(paths) == ppp:
            break
            
    key_pair = (int(source), int(target))
    return {key_pair: (paths, paths_nodes, paths_links_ids)}

def _all_simple_paths_dfs_trie(graph, links_map, ppp, node_pair):
    source = node_pair[0]
    target = node_pair[1]
    cutoff = calculate_cutoff(graph, source, target, k=2)
    paths = []
    paths_nodes = []
    paths_links_ids = []
    visited = [source]
    stack = [iter(graph[source])]
    trie = PathTrie(int(source), int(target))
    while stack:
        children = stack[-1]
        child = next(children, None)
        if child is None:
            stack.pop()
            visited.pop()
        elif len(visited) < cutoff:
            if child == target:
                new_path = visited + [target]
                int_path = [int(n) for n in new_path]
                # links = construct_links_from_path(new_path, links_map)
                # links_ids = [(l.src, l.dst) for l in links]
                trie.add(int_path)
                paths.append('1')
                # paths_nodes.append(int_path)
                # paths_links_ids.append(links_ids)
            elif child not in visited:
                visited.append(child)
                stack.append(iter(graph[child]))
        else:  # len(visited) == cutoff:
            if child == target or target in children:
                new_path = visited + [target]
                int_path = [int(n) for n in new_path]
                # links = construct_links_from_path(new_path, links_map)
                # links_ids = [(l.src, l.dst) for l in links]
                trie.add(int_path)
                paths.append('1')
                # paths_nodes.append(int_path)
                # paths_links_ids.append(links_ids)
            stack.pop()
            visited.pop()
        # if len(paths) > 2000:
        #     print 'XXX', source, target
    # paths = sorted(paths, key=lambda path: len(path))
    key_pair = (int(source), int(target))
    # return {key_pair: (paths, paths_nodes, paths_links_ids)}
    return {key_pair: trie}


def calculate_simple_paths_trie(graph, links_map, ppp, pool_size=4):
    p = Pool(pool_size)
    pmap = p.map
    pairs = [(n1, n2) for n1 in graph.nodes() for n2 in graph.nodes() if n1 != n2]
    fn = partial(_all_simple_paths_dfs_trie, graph, links_map, ppp)
    simple_paths = merge(pmap(fn, pairs))
    # print simple_paths[24, 17].nodes
    # for _x, _y in simple_paths[1, 12].nodes.iteritems():
    #     print _x, _y.counter, _y.end
    # simple_paths[1, 12].traverse()
    # for path_key, path_tuples in simple_paths.iteritems():
    #     paths = path_tuples[0]
    #     new_paths = []
    #     for path in paths:
    #         new_path = []
    #         for link in path:
    #             new_link = links_map[(link.src, link.dst)]
    #             new_path.append(new_link)
    #         new_paths.append(new_path)
    #     simple_paths[path_key] = (new_paths, path_tuples[1], path_tuples[2])
    p.close()
    return simple_paths

def paths_ratio(graph, links_map, ppp, pool_size=4):
    p = Pool(pool_size)
    pmap = p.map
    pairs = [(n1, n2) for n1 in graph.nodes() for n2 in graph.nodes() if n1 != n2]
    fn = partial(_all_simple_paths_dfs, graph, links_map, ppp)
    simple_paths = merge(pmap(fn, pairs))
    all_paths = 0.0
    ok_paths = 0.0
    count = 0.0
    summ = 0.0
    averages = []
    for path_key, path_tuples in simple_paths.iteritems():
        count += 1
        ok_paths += len(path_tuples[0])
        all_paths += path_tuples[3]
        summ += len(path_tuples[0]) / float(path_tuples[3])
        print "--", path_key, len(path_tuples[0]) / float(path_tuples[3])
        averages.append(len(path_tuples[0]) / float(path_tuples[3]))
    

    print ">>>>>>>>", ok_paths / all_paths
    print "Average of average", summ/ count
    print "MEAN", statistics.mean(averages)
    print "variance", statistics.variance(averages)
    p.close()
    return simple_paths

def calculate_simple_paths(graph, links_map, ppp, pool_size=4):
    p = Pool(pool_size)
    pmap = p.map
    pairs = [(n1, n2) for n1 in graph.nodes() for n2 in graph.nodes() if n1 != n2]
    fn = partial(_all_simple_paths_bfs_disjoint, graph, links_map, ppp)
    simple_paths = merge(pmap(fn, pairs))
    for path_key, path_tuples in simple_paths.iteritems():
        paths = path_tuples[0]
        new_paths = []
        for path in paths:
            new_path = []
            for link in path:
                new_link = links_map[(link.src, link.dst)]
                new_path.append(new_link)
            new_paths.append(new_path)
        simple_paths[path_key] = (new_paths, path_tuples[1], path_tuples[2])
    p.close()
    return simple_paths

def calculate_simple_paths_regenerate(graph, links_map, ppp, pool_size=4, pairs=[]):
    p = Pool(pool_size)
    pmap = p.map
    fn = partial(_all_simple_paths_bfs_disjoint, graph, links_map, ppp)
    simple_paths = merge(pmap(fn, pairs))
    for path_key, path_tuples in simple_paths.iteritems():
        paths = path_tuples[0]
        new_paths = []
        for path in paths:
            new_path = []
            for link in path:
                new_link = links_map[(link.src, link.dst)]
                new_path.append(new_link)
            new_paths.append(new_path)
        simple_paths[path_key] = (new_paths, path_tuples[1], path_tuples[2])
    p.close()
    return simple_paths

def calculate_simple_paths2(graph, links_map, ppp, pool_size=4):
    p = Pool(pool_size)
    pmap = p.map
    pairs = [(n1, n2) for n1 in graph.nodes() for n2 in graph.nodes() if n1 != n2]
    fn = partial(_all_simple_paths_disjoint, graph, links_map, ppp)
    simple_paths = merge(pmap(fn, pairs))
    for path_key, path_tuples in simple_paths.iteritems():
        paths = path_tuples[0]
        new_paths = []
        for path in paths:
            new_path = []
            for link in path:
                new_link = links_map[(link.src, link.dst)]
                new_path.append(new_link)
            new_paths.append(new_path)
        simple_paths[path_key] = (new_paths, path_tuples[1], path_tuples[2])
    p.close()
    return simple_paths

def calculate_simple_paths3(graph, links_map, ppp, pool_size=4):
    p = Pool(pool_size)
    pmap = p.map
    pairs = [(n1, n2) for n1 in graph.nodes() for n2 in graph.nodes() if n1 != n2]
    fn = partial(_all_simple_paths_dfs, graph, links_map, ppp)
    simple_paths = merge(pmap(fn, pairs))
    for path_key, path_tuples in simple_paths.iteritems():
        paths = path_tuples[0]
        new_paths = []
        for path in paths:
            new_path = []
            for link in path:
                new_link = links_map[(link.src, link.dst)]
                new_path.append(new_link)
            new_paths.append(new_path)
        simple_paths[path_key] = (new_paths, path_tuples[1], path_tuples[2])
    p.close()
    return simple_paths

def calculate_simple_paths_shrink(graph, links_map, ppp, pool_size=4):
    p = Pool(pool_size)
    pmap = p.map

    pairs = [(n1, n2) for n1 in graph.nodes() for n2 in graph.nodes() if n1 != n2]
    supernodes, shrink_graph = contract_linear_links(graph)

    fn = partial(_all_simple_paths_dfs_super, shrink_graph, graph, supernodes, links_map, ppp)
    simple_paths = merge(pmap(fn, pairs))
    for path_key, path_tuples in simple_paths.iteritems():
        paths = path_tuples[0]
        new_paths = []
        for path in paths:
            new_path = []
            for link in path:
                new_link = links_map[(link.src, link.dst)]
                new_path.append(new_link)
            new_paths.append(new_path)
        simple_paths[path_key] = (new_paths, path_tuples[1], path_tuples[2])
    p.close()
    return simple_paths


def add_path_to_graph(graph, path):
    i = 0
    j = 1
    while j < len(path):
        graph.add_edge(path[i], path[j])
        i += 1
        j += 1


def calculate_cutoff(graph, src, dst, k):
    cutoff = 100000
    try:
        shortest_path_len = nx.shortest_path_length(graph, src, dst)
        cutoff = int(k * shortest_path_len)

        # print cutoff, src, dst
        # if cutoff >= 20: # or cutoff <= 10:
        #     cutoff = 17
        # if cutoff < 10:
        #     cutoff = 10
        return min(cutoff, len(graph.nodes()) - 1)
    except:
        return cutoff
    

def all_simple_paths_dfs_compact(graph, k, node_pair):
    source = node_pair[0]
    target = node_pair[1]
    cutoff = calculate_cutoff(graph, source, target, k)
    new_graph = nx.DiGraph()
    visited = [source]
    stack = [iter(graph[source])]
    while stack:
        children = stack[-1]
        child = next(children, None)
        if child is None:
            stack.pop()
            visited.pop()
        elif len(visited) < cutoff:
            if child == target:
                new_path = visited + [target]
                add_path_to_graph(new_graph, new_path)
            elif child not in visited:
                visited.append(child)
                stack.append(iter(graph[child]))
        else:  # len(visited) == cutoff:
            if child == target or target in children:
                new_path = visited + [target]
                add_path_to_graph(new_graph, new_path)
            stack.pop()
            visited.pop()
    return {(int(source), int(target)): new_graph}


class PathTrieNode:
    def __init__(self, node_id, parent):
        self.node_id = node_id
        self.parent = parent
        self.children = {}
        self.link_ids = set()
        self.counter = 0
        # may not need the self.end
        self.end = False

    def contains(self, child_node_id):
        return child_node_id in self.children

    def add(self, child_node_id):
        self.children[child_node_id] = PathTrieNode(child_node_id, self)
        self.link_ids.add((self.node_id, child_node_id))
        return self.children[child_node_id]

    def get(self, child_node_id):
        return self.children.get(child_node_id, None)


class _OkTrieTraversalContext:
    def __init__(self, path_count):
        self.path_idx = 0
        self.link_cache = defaultdict(float)
        self.node_cache = defaultdict(float)
        self.final_cost = [float(maxint)] * path_count


class PathTrie:
    def __init__(self, source, target):
        self.source = source
        self.target = target

        self.root = PathTrieNode(source, parent=None)
        self.nodes = {self.root.node_id: self.root}
        self.link_ids = set()
        self.current_traversed_path_idx = 0
        self.path_count = 0

    def add(self, path):
        self.path_count += 1
        curr = self.root
        for node_id in path[1:]:
            if not curr.contains(node_id):
                new_node = curr.add(node_id)
                self.nodes[new_node.node_id] = new_node
            self.link_ids.update(curr.link_ids)
            curr.counter += 1
            curr = curr.get(node_id)
        self.nodes[path[-2]].end = True

    def _do_traverse(self, node, path, path_cost, ok_ctx):
        if node.node_id == self.target:
            ok_ctx.final_cost[ok_ctx.path_idx] = path_cost
            assert path_cost == (2*len(path) - 1), "%d, %d" % (path_cost, len(path))
            ok_ctx.path_idx += 1

        node_cost = ok_ctx.node_cache.get(node.node_id, 1)
        ok_ctx.node_cache[node.node_id] = node_cost
        path_cost += node_cost

        if node.node_id == 4:
            return

        for child_id, child in node.children.items():
            if child.node_id == self.target:
                node_cost = ok_ctx.node_cache.get(child.node_id, 1)
                ok_ctx.node_cache[child.node_id] = node_cost
                path_cost += node_cost

            link_id = (node.node_id, child.node_id)
            link_cost = ok_ctx.link_cache.get(link_id, 1)
            ok_ctx.link_cache[link_id] = link_cost
            path_cost += link_cost

            self._do_traverse(child, path=path + [child.node_id], path_cost=path_cost, ok_ctx=ok_ctx)

            path_cost -= link_cost

            if child.node_id == self.target:
                node_cost = ok_ctx.node_cache.get(child.node_id, 1)
                ok_ctx.node_cache[child.node_id] = node_cost
                path_cost -= node_cost

    def traverse(self):
        ok_ctx = _OkTrieTraversalContext(self.path_count)
        path = [self.root.node_id]
        path_cost = 0
        self._do_traverse(self.root, path, path_cost, ok_ctx=ok_ctx)
        print ok_ctx.final_cost
        ok_ctx.path_idx = 0
