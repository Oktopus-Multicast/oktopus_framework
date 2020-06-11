

def _dump_model(cls, obj_list, file_path):
    assert obj_list is not None
    assert isinstance(obj_list, list)

    if hasattr(cls, 'csv_header'):
        header = cls.csv_header()
        try:
            with open(file_path, 'wb') as fp:
                fp.write(header)
                fp.write('\n')

                for obj in obj_list:
                    if hasattr(obj, 'csv'):
                        fp.write(obj.csv())
                        fp.write('\n')
        except (OSError, IOError) as e:
            print e.message


def dump_objects(cls, file_path, objects):
    _dump_model(cls=cls, obj_list=objects, file_path=file_path)


# def dump_servers(file_path, servers):
#     _dump_model(Server, servers, file_path)
#
#
# def dump_versions(file_path, versions):
#     _dump_model(VersionInfo, versions, file_path)
#
#
# def dump_links(file_path, links):
#     _dump_model(Link, links, file_path)
#
#
# def dump_vs_info(file_path, vs_info):
#     _dump_model(VersionServerInfo, vs_info, file_path)
#
#
# def dump_requests(file_path, timeline):
#     assert isinstance(timeline, dict)
#
#     for server_id, requests_list in timeline.iteritems():
#         server_file_path = file_path % server_id
#         _dump_model(cls=Request, obj_list=requests_list, file_path=server_file_path)
#
#
# def dump_sessions(file_path, sessions):
#     _dump_model(cls=Session, obj_list=sessions, file_path=file_path)
#
#
# def dump_session_events(file_path, session_events):
#     _dump_model(SessionEventTrigger, session_events, file_path)
