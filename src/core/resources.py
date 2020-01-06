from typing import List, Dict
import copy


class Resource:
    def __init__(self):
        pass

    def satisfy(self, cpu) -> bool:
        raise NotImplementedError('Not implemented')
        return False

    def alloc(self, resource):
        raise NotImplementedError('Not implemented')

    def dealloc(self, resource):
        raise NotImplementedError('Not implemented')


class Cpu(Resource):
    def __init__(self, cpu):
        self.cpu = cpu
        self.remaining = cpu

    def satisfy(self, cpu):
        return self.remaining >= cpu

    def alloc(self, cpu) -> float:
        assert self.satisfy(cpu), 'Cpu resource not available'
        self.remaining -= cpu
        return cpu

    def dealloc(self, cpu):
        assert self.remaining+cpu <= self.cpu, \
            'Error dealloc resources: remaining more than original'
        self.remaining += cpu


class Mem(Resource):
    def __init__(self, mem):
        self.mem = mem
        self.remaining = mem

    def satisfy(self, mem):
        return self.remaining >= mem

    def alloc(self, mem) -> float:
        assert self.satisfy(mem), 'Mem resource not available'
        self.remaining -= mem

    def dealloc(self, mem):
        assert self.remaining+mem <= self.mem, \
            'Error dealloc resources: remaining more than original'
        self.remaining += mem


class Gpu(Resource):
    def __init__(self, gpus: List[float]):
        """ Gpu resource is modeled as list of gpu memory of all gpus on a node.
        """
        self.gpus = gpus
        self.remaining = copy.deepcopy(gpus)

    def __repr__(self):
        return '<GPU res, max: {}, remain: {}>'.format(self.gpus, self.remaining)

    def satisfy(self, gpus: List[float]):
        """ gpu requests are modeled as a list of memory requested for each gpu,
        assuming allocations are all from distinct gpus
        """
        # get the sorted remaining and requested memory, and see if all requests
        # can be satisfied
        availables = sorted(self.remaining, reverse=True)
        requests = sorted(gpus, reverse=True)

        for i, req in enumerate(requests):
            if availables[i] < req:
                return False
        return True

    def alloc(self, gpus: List[float]) -> List[float]:
        assert self.satisfy(gpus), 'Gpu resource not available'

        alloc = [0.0 for _ in self.gpus]
        for req in gpus:
            found = False

            for i, resource in enumerate(self.remaining):
                if req <= resource:
                    self.remaining[i] -= req
                    alloc[i] = req
                    found = True
                    break

            assert found, 'Gpu resource could not be allocated'

        return alloc

    def dealloc(self, gpus: List[float]):
        for i, gpu in enumerate(gpus):
            self.remaining[i] += gpu
            assert self.remaining[i] <= self.gpus[i], \
                'Error dealloc resources: remaining more than original'


class Node:
    def __init__(self, node_id, resources: Dict[str, Resource]):
        self.simulator = None
        self.node_id = node_id
        self.resources = resources

    def __repr__(self):
        return 'Node {} with resources {}'.format(self.node_id, self.resources)

    def satisfy(self, resources: Dict[str, Resource]):
        return all(self.resources[name].satisfy(resource)
                   for name, resource in resources.items())

    def alloc(self, resources: Dict[str, Resource]):
        ret = {}
        for name, resource in resources.items():
            ret[name] = self.resources[name].alloc(resource)

        return ret

    def dealloc(self, resources: Dict[str, Resource]):
        for name, resource in resources.items():
            self.resources[name].dealloc(resource)
