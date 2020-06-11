from solution import Solution


class RoutingTechnology:
    def __init__(self, name, solution):
        assert isinstance(solution, Solution)
        self.name = name
        self.solution = solution

    def encode(self):
        raise NotImplementedError('Please use a specialized class...')


class OFRoutingTechnology(RoutingTechnology):
    def __init__(self, solution):
        RoutingTechnology.__init__(self, name='OF1.3', solution=solution)

    def encode(self):
        pass


class SRMcastRoutingTechnology(RoutingTechnology):
    def __init__(self, solution):
        RoutingTechnology.__init__(self, name='SRMcast', solution=solution)

    def encode(self):
        pass

