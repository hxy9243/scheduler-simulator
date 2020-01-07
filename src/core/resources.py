from typing import List, Dict
import copy

from collections import defaultdict


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

    def utilization(self) -> float:
        raise NotImplementedError('Not implemented')
        return 0.0


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

    def utilization(self) -> float:
        return (self.cpu - self.remaining) / self.cpu


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

    def utilization(self) -> float:
        return (self.mem - self.remaining) / self.mem


class Gpu(Resource):
    def __init__(self, gpus: List[float]):
        """ Gpu resource is modeled as list of gpu memory of all gpus on a node.
        """
        self.gpus = gpus
        self.remaining = copy.deepcopy(gpus)

    def __repr__(self):
        return '<GPU res, max: {}, remain: {}>'.format(self.gpus, self.remaining)

    def satisfy(self, request):
        """ gpu requests are modeled as a list of memory requested for each gpu,
        assuming allocations are all from distinct gpus
        """
        # get the sorted remaining and requested memory, and see if all requests
        # can be satisfied
        availables = sorted(self.remaining, reverse=True)
        requests = sorted(request.gpus, reverse=True)

        for i, req in enumerate(requests):
            if availables[i] < req:
                return False
        return True

    def alloc(self, request):
        assert self.satisfy(request), 'Gpu resource not available'

        alloc = [0.0 for _ in self.gpus]
        alloced = set()

        for req in request.gpus:
            found = False

            for i, resource in enumerate(self.remaining):
                if i not in alloced and req <= resource:
                    self.remaining[i] -= req
                    alloc[i] = req
                    alloced.add(i)
                    found = True
                    break

            assert found, 'Gpu resource could not be allocated'

        print('available', self.remaining,
              'requests', request, 'alloced', alloc)

        return Gpu(alloc)

    def dealloc(self, request):
        for i, gpu in enumerate(request.gpus):
            self.remaining[i] += gpu
            assert self.remaining[i] <= self.gpus[i], \
                'Error dealloc resources: remaining more than original'

    def utilization(self) -> float:
        return (sum(self.gpus) - sum(self.remaining)) / sum(self.gpus)


class Node:
    def __init__(self, node_id, resources: Dict[str, Resource]):
        self.simulator = None
        self.node_id = node_id
        self.resources = resources

        # gather statistics about the node
        self.tasks = 0
        self.records = defaultdict(lambda: [(0, 0.0)])

    def __repr__(self):
        return 'Node {} with resources {}'.format(self.node_id, self.resources)

    def satisfy(self, resources: Dict[str, Resource]):
        return all(self.resources[name].satisfy(resource)
                   for name, resource in resources.items())

    def alloc(self, resources: Dict[str, Resource]):
        ret = {}
        for name, resource in resources.items():
            ret[name] = self.resources[name].alloc(resource)

        self.tasks += 1
        self.record('tasks', self.tasks)

        print('tasks', self.tasks)

        self.record('gpu-util', self.resources['gpu'].utilization())

        return ret

    def dealloc(self, resources: Dict[str, Resource]):
        for name, resource in resources.items():
            self.resources[name].dealloc(resource)

        self.tasks -= 1
        self.record('tasks', self.tasks)
        self.record('gpu-util', self.resources['gpu'].utilization())

    def record(self, key: str, value: float):
        #print((self.simulator.env.now, self.resources['gpu']))

        self.records[key].append((self.simulator.env.now, value))

    def get_records(self):
        return self.records
