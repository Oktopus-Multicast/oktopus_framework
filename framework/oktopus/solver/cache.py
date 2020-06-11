import os
import cPickle as pickle
from multiprocessing import Pool

from cytoolz import merge, partial

from oktopus_utils import all_simple_paths_dfs_compact


def ensure_dir(directory):
    if not os.path.isdir(directory) or not os.path.exists(directory):
        os.makedirs(directory)


class TopoCache:
    def __init__(self, name, directory):
        self.name = name
        self.directory = directory

    def read(self):
        raise NotImplementedError

    def invalidate(self):
        raise NotImplementedError


class CompactTopoCache(TopoCache):
    PKL_FILE = 'compact.pkl'

    def __init__(self, name, directory, graph, k, pool_size):
        TopoCache.__init__(self, name, directory)
        self._graph = graph
        self._k = k
        self._pool_size = pool_size

        self._cache_dir = os.path.join(self.directory, self.name, 'compact', str(self._k))
        ensure_dir(self._cache_dir)
        self._pkl_file = os.path.join(self._cache_dir, self.PKL_FILE)

    def _write(self):
        print '[compact] generating a new cache'
        p = Pool(self._pool_size)
        pmap = p.map
        pairs = [(n1, n2) for n1 in self._graph.nodes() for n2 in self._graph.nodes() if n1 != n2]
        fn = partial(all_simple_paths_dfs_compact, self._graph, self._k)
        compact_graphs = merge(pmap(fn, pairs))
        with open(self._pkl_file, 'wb') as fp:
            print '[compact] write cache to:', self._pkl_file
            pickle.dump(compact_graphs, fp)
        p.close()
        return compact_graphs

    def read(self):
        if os.path.exists(self._pkl_file) and os.path.isfile(self._pkl_file):
            print '[compact] read cache from:', self._pkl_file
            with open(self._pkl_file, 'rb') as fp:
                compact_graphs = pickle.load(fp)
            return compact_graphs
        else:
            return self._write()

    def invalidate(self):
        if os.path.exists(self._pkl_file) and os.path.isfile(self._pkl_file):
            os.remove(self._pkl_file)
