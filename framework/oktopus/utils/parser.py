from ..dataset import parse_objects
from ..multicast.network import Node, Link
from ..multicast.session import Session


def load_sessions(file_path):
    return parse_objects(file_path, cls=Session)


def load_nodes(file_path):
    return parse_objects(file_path, cls=Node)


def load_links(file_path):
    return parse_objects(file_path, cls=Link)
