from cpython cimport bool


cdef class SSPResult:
    cdef public bool found
    cdef public float cost
    cdef public list path
    cdef public list path_nodes
    cdef public object srv_map
    cdef public bool optimized


cdef class CyOktopusSolver:
    cdef public str name
    cdef public object app
    cdef public dict options

    cdef public object _solution
    cdef public int _cutoff

    cdef public dict _simple_paths 
    cdef public dict _simple_paths_nodes 
    cdef public dict _simple_paths_links_ids 
    cdef public dict _links_map 
    cdef public dict _delay_map 
    cdef public dict _invalid_delay_map 
    cdef public dict _pass_map_src

    cdef:
        void _init_oktopus(self)
        void _init_delay_maps(self)
        tuple _find_simple_service_path(self, session, int dst, tree)


    cpdef object optimize(self)

