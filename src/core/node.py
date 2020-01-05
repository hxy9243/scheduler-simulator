class Node:
    def __init__(self, resources):
        self.simulator = None
        self.resources = resources


class Resource:
    pass


class Cpu(Resource):
    def __init__(self):
        pass


class Mem(Resource):
    def __init__(self):
        pass


class Gpu(Resource):
    def __init__(self):
        pass


class Allocation:
    def __init__(self):
        pass
