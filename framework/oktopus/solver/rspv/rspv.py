from ..base import Solver


class RSVPSolver(Solver):
    def __init__(self, app, **kwargs):
        Solver.__init__(self, name='rsvp', app=app, **kwargs)

    def optimize(self):
        pass
