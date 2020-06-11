import csv
SERVERS_FILE = 'servers.csv'
VERSIONS_FILE = 'versions.csv'
LINKS_FILE = 'links.csv'
VS_INFO_FILE = 'vs_info.csv'
SESSIONS_FILE = 'sessions_%s.csv'
SESSION_EVENTS_FILE = 'session_events_%s.csv'
REQUESTS_FILE = 'requests_%s.csv'
REQUESTS_DEVIATE_FILE = 'requests_deviate_%s.csv'
DAY_FORMAT = 'day-%s'


def _understand_model(cls):
    attributes = {}
    default_types = {'str': str, 'int': int, 'float': float, 'bool': bool}
    if hasattr(cls, 'MODEL'):
        if ',' in cls.MODEL:
            splitted_model = cls.MODEL.split(',')
            for attribute in splitted_model:
                if ':' in attribute:
                    splitted_attr = attribute.split(':')
                    a_name = splitted_attr[0]
                    a_type = default_types.get(splitted_attr[1], splitted_attr[1])
                    attributes['{'+a_name+'}'] = a_type
    return attributes


def _read_lines(file_path):
    lines = []
    try:
        with open(file_path, 'rb') as fp:
            lines = fp.readlines()
        lines = [line.strip() for line in lines]
    except (OSError, IOError) as e:
        pass

    return lines


def _parse_model(file_path, cls, handlers=None):
    if not handlers:
        handlers = {}
    objects = []
    objects_map = {}
    attributes = _understand_model(cls)
    keys = cls.FORMAT.replace('{', '').replace('}', '')
    keys_list = keys.split(',')
    splitted_format = cls.FORMAT.split(',')

    with open(file_path, 'rb') as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        next(reader, None)
        for line in reader:
            if len(line) == len(splitted_format):
                values = []
                for attr_value, attr_name in zip(line, splitted_format):
                    if attr_name in attributes:
                        attr_type = attributes[attr_name]
                        if attr_type in handlers:
                            parsed_attr = handlers[attr_type](attr_value)
                        elif attr_type == cls.__name__:
                            parsed_attr = objects_map.get(attr_value, None)
                        elif attr_type == 'list':
                            if attr_value == '':
                                attr_value = "()"
                            parsed_attr = eval(attr_value)
                        else:
                            parsed_attr = attr_type(attr_value)
                        values.append(parsed_attr)
                attr_dict = dict(zip(keys_list, values))
                new_obj = cls(**attr_dict)
                obj_id = getattr(new_obj, keys_list[0])
                objects_map[obj_id] = new_obj
                objects.append(new_obj)
            else:
                print '?'
    return objects


def parse_objects(file_path, cls):
    handlers = {bool: lambda x: True if x == 'True' else False}
    return _parse_model(file_path, cls, handlers)

# def parse_servers(file_path):
#     handlers = {bool: lambda x: True if x == 'True' else False}
#     return _parse_model(file_path, Server, handlers)
#
#
# def parse_links(file_path, servers):
#     servers_map = {server.s_id: server for server in servers}
#     handlers = {bool: lambda x: True if x == 'True' else False,
#                 'Server': lambda x: servers_map.get(x, None)}
#     return _parse_model(file_path, Link, handlers)
#
#
# def parse_versions(file_path):
#     handlers = {bool: lambda x: True if x == 'True' else False}
#     return _parse_model(file_path, VersionInfo, handlers)
#
#
# def parse_vs_info(file_path, servers, versions):
#     servers_map = {server.s_id: server for server in servers}
#     versions_map = {version.v_id: version for version in versions}
#     handlers = {bool: lambda x: True if x == 'True' else False,
#                 'Server': lambda x: servers_map.get(x, None),
#                 'VersionInfo': lambda x: versions_map.get(x, None)}
#
#     return _parse_model(file_path, VersionServerInfo, handlers)
#
#
# def parse_requests(file_path, versions):
#     versions_map = {version.v_id: version for version in versions}
#     handlers = {bool: lambda x: True if x == 'True' else False,
#                 'VersionInfo': lambda x: versions_map.get(x, None)}
#     return _parse_model(file_path, Request, handlers)


# def parse_session_events(file_path, sessions):
#     sessions_map = {session.address: session for session in sessions}
#     handlers = {bool: lambda x: True if x == 'True' else False,
#                 'Session': lambda x: sessions_map.get(x, None)}
#     return _parse_model(file_path, SessionEventTrigger, handlers)
