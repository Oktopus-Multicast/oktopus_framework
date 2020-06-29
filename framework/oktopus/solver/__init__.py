from .oktopus import OktopusSolver
from .mldp import MLDPSolver
from .rspv import RSVPSolver
from .mtrsa import MTRSASolver
from .msa import MSASolver
from .cplex import CPLEXMTESolver, CPLEXSCSolver

from technology import OFRoutingTechnology, SRMcastRoutingTechnology



__all__ = ['OktopusSolver', 'MLDPSolver', 'RSVPSolver',
           'OFRoutingTechnology', 'SRMcastRoutingTechnology', 'MTRSASolver', 'MSASolver', 'CPLEXMTESolver', 'CPLEXSCSolver']

ALGO_MAP = {
    'mldp': MLDPSolver,
    'rspv': RSVPSolver,
    'mtrsa': MTRSASolver,
    'msa' : MSASolver,
    'cplex_mte': CPLEXMTESolver,
    'cplex_sc': CPLEXSCSolver
}