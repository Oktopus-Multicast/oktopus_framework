# disjoint_paths.py - Flow based node and edge disjoint paths.
#
# Copyright 2017-2019 NetworkX developers.
#
# This file is part of NetworkX.
#
# NetworkX is distributed under a BSD license; see LICENSE.txt for more
# information.
#
# Author: Jordi Torrents <jordi.t21@gmail.com>
"""Flow based node and edge disjoint paths."""
import networkx as nx
from networkx.exception import NetworkXNoPath
# Define the default maximum flow function to use for the undelying
# maximum flow computations
from networkx.algorithms.flow import edmonds_karp
from networkx.algorithms.flow import preflow_push
from networkx.algorithms.flow import shortest_augmenting_path
default_flow_func = edmonds_karp
# Functions to build auxiliary data structures.
from networkx.algorithms.flow import build_residual_network

try:
    from itertools import filterfalse as _filterfalse
except ImportError:  # Python 2
    def _filterfalse(predicate, iterable):
        # https://docs.python.org/3/library/itertools.html
        # filterfalse(lambda x: x%2, range(10)) --> 0 2 4 6 8
        if predicate is None:
            predicate = bool
        for x in iterable:
            if not predicate(x):
                yield x

__all__ = [
    'edge_disjoint_paths'
]

def build_auxiliary_node_connectivity(G):
    r"""Creates a directed graph D from an undirected graph G to compute flow
    based node connectivity.
    For an undirected graph G having `n` nodes and `m` edges we derive a
    directed graph D with `2n` nodes and `2m+n` arcs by replacing each
    original node `v` with two nodes `vA`, `vB` linked by an (internal)
    arc in D. Then for each edge (`u`, `v`) in G we add two arcs (`uB`, `vA`)
    and (`vB`, `uA`) in D. Finally we set the attribute capacity = 1 for each
    arc in D [1]_.
    For a directed graph having `n` nodes and `m` arcs we derive a
    directed graph D with `2n` nodes and `m+n` arcs by replacing each
    original node `v` with two nodes `vA`, `vB` linked by an (internal)
    arc (`vA`, `vB`) in D. Then for each arc (`u`, `v`) in G we add one 
    arc (`uB`, `vA`) in D. Finally we set the attribute capacity = 1 for
    each arc in D.
    A dictionary with a mapping between nodes in the original graph and the
    auxiliary digraph is stored as a graph attribute: H.graph['mapping'].
    References
    ----------
    .. [1] Kammer, Frank and Hanjo Taubig. Graph Connectivity. in Brandes and
        Erlebach, 'Network Analysis: Methodological Foundations', Lecture
        Notes in Computer Science, Volume 3418, Springer-Verlag, 2005.
        http://www.informatik.uni-augsburg.de/thi/personen/kammer/Graph_Connectivity.pdf
    """
    directed = G.is_directed()

    mapping = {}
    H = nx.DiGraph()

    for i, node in enumerate(G):
        mapping[node] = i
        H.add_node('%dA' % i, id=node)
        H.add_node('%dB' % i, id=node)
        H.add_edge('%dA' % i, '%dB' % i, capacity=1)

    edges = []
    for (source, target) in G.edges():
        edges.append(('%sB' % mapping[source], '%sA' % mapping[target]))
        if not directed:
            edges.append(('%sB' % mapping[target], '%sA' % mapping[source]))
    H.add_edges_from(edges, capacity=1)

    # Store mapping as graph attribute
    H.graph['mapping'] = mapping
    return H


def build_auxiliary_edge_connectivity(G):
    """Auxiliary digraph for computing flow based edge connectivity
    If the input graph is undirected, we replace each edge (`u`,`v`) with
    two reciprocal arcs (`u`, `v`) and (`v`, `u`) and then we set the attribute
    'capacity' for each arc to 1. If the input graph is directed we simply
    add the 'capacity' attribute. Part of algorithm 1 in [1]_ .
    References
    ----------
    .. [1] Abdol-Hossein Esfahanian. Connectivity Algorithms. (this is a
        chapter, look for the reference of the book).
        http://www.cse.msu.edu/~cse835/Papers/Graph_connectivity_revised.pdf
    """
    if G.is_directed():
        H = nx.DiGraph()
        H.add_nodes_from(G.nodes())
        H.add_edges_from(G.edges(), capacity=1)
        return H
    else:
        H = nx.DiGraph()
        H.add_nodes_from(G.nodes())
        for (source, target) in G.edges():
            H.add_edges_from([(source, target), (target, source)], capacity=1)
        return H

def edge_disjoint_paths(G, s, t, flow_func=None, cutoff=None, auxiliary=None,
                        residual=None):
    """Returns the edges disjoint paths between source and target.

    Edge disjoint paths are paths that do not share any edge. The
    number of edge disjoint paths between source and target is equal
    to their edge connectivity.

    Parameters
    ----------
    G : NetworkX graph

    s : node
        Source node for the flow.

    t : node
        Sink node for the flow.

    flow_func : function
        A function for computing the maximum flow among a pair of nodes.
        The function has to accept at least three parameters: a Digraph, 
        a source node, and a target node. And return a residual network 
        that follows NetworkX conventions (see :meth:`maximum_flow` for 
        details). If flow_func is None, the default maximum flow function 
        (:meth:`edmonds_karp`) is used. The choice of the default function
        may change from version to version and should not be relied on.
        Default value: None.

    cutoff : int
        Maximum number of paths to yield. Some of the maximum flow
        algorithms, such as :meth:`edmonds_karp` (the default) and 
        :meth:`shortest_augmenting_path` support the cutoff parameter,
        and will terminate when the flow value reaches or exceeds the
        cutoff. Other algorithms will ignore this parameter.
        Default value: None.

    auxiliary : NetworkX DiGraph
        Auxiliary digraph to compute flow based edge connectivity. It has
        to have a graph attribute called mapping with a dictionary mapping
        node names in G and in the auxiliary digraph. If provided
        it will be reused instead of recreated. Default value: None.

    residual : NetworkX DiGraph
        Residual network to compute maximum flow. If provided it will be
        reused instead of recreated. Default value: None.

    Returns
    -------
    paths : generator
        A generator of edge independent paths.

    Raises
    ------
    NetworkXNoPath : exception
        If there is no path between source and target.

    NetworkXError : exception
        If source or target are not in the graph G.

    See also
    --------
    :meth:`node_disjoint_paths`
    :meth:`edge_connectivity`
    :meth:`maximum_flow`
    :meth:`edmonds_karp`
    :meth:`preflow_push`
    :meth:`shortest_augmenting_path`

    Examples
    --------
    We use in this example the platonic icosahedral graph, which has node
    edge connectivity 5, thus there are 5 edge disjoint paths between any
    pair of nodes.

    >>> G = nx.icosahedral_graph()
    >>> len(list(nx.edge_disjoint_paths(G, 0, 6)))
    5


    If you need to compute edge disjoint paths on several pairs of
    nodes in the same graph, it is recommended that you reuse the
    data structures that NetworkX uses in the computation: the 
    auxiliary digraph for edge connectivity, and the residual
    network for the underlying maximum flow computation.

    Example of how to compute edge disjoint paths among all pairs of
    nodes of the platonic icosahedral graph reusing the data 
    structures.

    >>> import itertools
    >>> # You also have to explicitly import the function for 
    >>> # building the auxiliary digraph from the connectivity package
    >>> from networkx.algorithms.connectivity import (
    ...     build_auxiliary_edge_connectivity)
    >>> H = build_auxiliary_edge_connectivity(G)
    >>> # And the function for building the residual network from the
    >>> # flow package
    >>> from networkx.algorithms.flow import build_residual_network
    >>> # Note that the auxiliary digraph has an edge attribute named capacity
    >>> R = build_residual_network(H, 'capacity')
    >>> result = {n: {} for n in G}
    >>> # Reuse the auxiliary digraph and the residual network by passing them
    >>> # as arguments
    >>> for u, v in itertools.combinations(G, 2):
    ...     k = len(list(nx.edge_disjoint_paths(G, u, v, auxiliary=H, residual=R)))
    ...     result[u][v] = k
    >>> all(result[u][v] == 5 for u, v in itertools.combinations(G, 2))
    True

    You can also use alternative flow algorithms for computing edge disjoint
    paths. For instance, in dense networks the algorithm
    :meth:`shortest_augmenting_path` will usually perform better than
    the default :meth:`edmonds_karp` which is faster for sparse
    networks with highly skewed degree distributions. Alternative flow
    functions have to be explicitly imported from the flow package.

    >>> from networkx.algorithms.flow import shortest_augmenting_path
    >>> len(list(nx.edge_disjoint_paths(G, 0, 6, flow_func=shortest_augmenting_path)))
    5

    Notes
    -----
    This is a flow based implementation of edge disjoint paths. We compute
    the maximum flow between source and target on an auxiliary directed
    network. The saturated edges in the residual network after running the
    maximum flow algorithm correspond to edge disjoint paths between source
    and target in the original network. This function handles both directed
    and undirected graphs, and can use all flow algorithms from NetworkX flow
    package.

    """
    if s not in G:
        raise nx.NetworkXError('node %s not in graph' % s)
    if t not in G:
        raise nx.NetworkXError('node %s not in graph' % t)

    if flow_func is None:
        flow_func = default_flow_func

    if auxiliary is None:
        H = build_auxiliary_edge_connectivity(G)
    else:
        H = auxiliary

    # Maximum possible edge disjoint paths
    possible = min(H.out_degree(s), H.in_degree(t))
    if not possible:
        raise NetworkXNoPath

    if cutoff is None:
        cutoff = possible
    else:
        cutoff = min(cutoff, possible)

    # Compute maximum flow between source and target. Flow functions in
    # NetworkX return a residual network.
    kwargs = dict(capacity='capacity', residual=residual, cutoff=cutoff,
                  value_only=True)
    if flow_func is preflow_push:
        del kwargs['cutoff']
    if flow_func is shortest_augmenting_path:
        kwargs['two_phase'] = True
    R = flow_func(H, s, t, **kwargs)

    if R.graph['flow_value'] == 0:
        raise NetworkXNoPath

    # Saturated edges in the residual network form the edge disjoint paths
    # between source and target
    cutset = [(u, v) for u, v, d in R.edges(data=True)
              if d['capacity'] == d['flow'] and d['flow'] > 0]
    # This is equivalent of what flow.utils.build_flow_dict returns, but
    # only for the nodes with saturated edges and without reporting 0 flows.
    flow_dict = {n: {} for edge in cutset for n in edge}
    for u, v in cutset:
        flow_dict[u][v] = 1

    # Rebuild the edge disjoint paths from the flow dictionary.
    paths_found = 0
    for v in list(flow_dict[s]):
        if paths_found >= cutoff:
            # preflow_push does not support cutoff: we have to
            # keep track of the paths founds and stop at cutoff.
            break
        path = [s]
        if v == t:
            path.append(v)
            yield path
            continue
        u = v
        while u != t:
            path.append(u)
            try:
                u, _ = flow_dict[u].popitem()
            except KeyError:
                break
        else:
            path.append(t)
            yield path
            paths_found += 1