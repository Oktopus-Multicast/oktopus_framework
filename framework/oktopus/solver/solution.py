from ..multicast.session import Session


class _OkNode:
    def __init__(self, node_id, parent=None, children=None):
        self.node_id = node_id
        self.parent = parent
        self.children = children if children is not None else []
        self.services = []

    def __str__(self):
        return '_OkNode (node_id={})'.format(self.node_id)


class _OkTree:
    def __init__(self, root):
        self._session = None
        if isinstance(root, _OkNode):
            self.root = root
        elif isinstance(root, Session):
            self.root = _OkNode(root.src)
            self._session = root
        else:
            raise ValueError('_OkNode or Session only...')
        self._nodes = {self.root.node_id: self.root}
        self._leaves = {}
        self._links = []
        self.last_nodes_ids = set()
        self._node_paths = []
        self._node_paths_services = []
        # a map where each key is a node id, and the value is the distance from the session SOURCE to that node
        self._hop_count_map = {self.root.node_id: 0}
        self._max_hop_count = 0
        self.joined_receivers = set()
        self.has_path = False

    def get_max_hop_count(self):
        return self._max_hop_count

    def get_hop_count(self, node_id):
        return self._hop_count_map.get(node_id, -1)

    def get_links_ids(self):
        return self._links

    def get_leaves_ids(self):
        return [l.node_id for l in self._leaves]

    def get_nodes_ids(self):
        return self._nodes.keys()

    def get_node(self, node_id):
        return self._nodes.get(node_id, None)

    def get_node_paths(self):
        return self._node_paths

    def get_node_paths_services(self):
        return self._node_paths_services

    def add_path(self, path, node_srv_map, dsts):
        node_srv_map = {} if not node_srv_map else node_srv_map

        # print '[OkTree] adding path:', path
        parent = None
        for node_id_idx, node_id in enumerate(path):
            if node_id not in self._nodes:
                self._nodes[node_id] = _OkNode(node_id, parent=parent)
            if node_id in dsts:
                self.joined_receivers.add(node_id)
            ok_node = self._nodes[node_id]
            parent = ok_node
            if node_id in node_srv_map:
                # print node_id, node_srv_map[node_id]
                ok_node.services.extend(node_srv_map[node_id])

            # set the hop_count_map
            offset = self._hop_count_map[path[0]]
            self._hop_count_map[node_id] = node_id_idx + offset
            if self._hop_count_map[node_id] > self._max_hop_count:
                self._max_hop_count = self._hop_count_map[node_id]

        self.last_nodes_ids.add(path[-1])

        i = 0
        j = 1
        while j < len(path):
            curr_parent = self._nodes[path[i]]
            curr_child = self._nodes[path[j]]
            if curr_child not in curr_parent.children:
                curr_parent.children.append(curr_child)
            if (path[i], path[j]) not in self._links:
                self._links.append((path[i], path[j]))
            i += 1
            j += 1

        ok_nodes_list = [self._nodes[n_id] for n_id in path]
        self._node_paths.append(ok_nodes_list)

        # appending node id of last service
        if node_srv_map:
            self._node_paths_services.append(node_srv_map.keys()[-1])
        else:
            self._node_paths_services.append(-1)
        for rec_node_id in self.joined_receivers:
            if rec_node_id not in self._leaves:
                self._leaves[rec_node_id] = self._nodes[rec_node_id]
        self.has_path = True

    def traverse(self, root=None):
        if not root:
            root = self.root
        output_nodes = []
        output_links = []
        stack = [root]
        while len(stack) > 0:
            node = stack.pop()
            neighbors = node.children
            output_nodes.append(node.node_id)
            if neighbors:
                for n in neighbors:
                    output_links.append((node.node_id, n.node_id))
                    r_output_nodes, r_output_links = self.traverse(n)
                    output_nodes.extend(r_output_nodes)
                    output_links.extend(r_output_links)
        return output_nodes, output_links


class Solution:
    def __init__(self):
        self.trees = {}
        self.total_time = -1

    def get_new_tree(self, session):
        if session.addr in self.trees:
            del self.trees[session.addr]
        return self.get_tree(session)

    def get_tree(self, session):
        if session.addr not in self.trees:
            self.trees[session.addr] = _OkTree(session)
        return self.trees[session.addr]

    def add_path(self, session, path, node_srv_map):
        self.get_tree(session).add_path(path, node_srv_map, session.dsts)

    def is_joined(self, session, dst):
        return dst in self.get_tree(session).joined_receivers

    def is_session_done(self, session):
        tree = self.get_tree(session)
        return sorted(list(tree.joined_receivers)) == sorted(session.dsts)
