from typing import List, Dict, Tuple, Set, Any, Optional
import copy
from dataclasses import dataclass

from simpy import Environment

from pandas import DataFrame

ResourcesMapType = Dict[str, 'Resource']


class Resource:
    def __init__(self):
        pass

    def satisfy(self, cpu) -> bool:
        raise NotImplementedError('Not implemented')
        return False

    def alloc(self, resource) -> Any:
        raise NotImplementedError('Not implemented')

    def dealloc(self, resource) -> None:
        raise NotImplementedError('Not implemented')

    def utilization(self) -> float:
        raise NotImplementedError('Not implemented')
        return 0.0


class Cpu(Resource):
    def __init__(self, cpu: float):
        self.cpu = cpu
        self.remaining = cpu

    def satisfy(self, cpu: float) -> bool:
        return self.remaining >= cpu

    def alloc(self, cpu: float) -> float:
        assert self.satisfy(cpu), 'Cpu resource not available'
        self.remaining -= cpu
        return cpu

    def dealloc(self, cpu: float):
        assert self.remaining + cpu <= self.cpu, \
            'Error dealloc resources: remaining more than original'
        self.remaining += cpu

    def utilization(self) -> float:
        return (self.cpu - self.remaining) / self.cpu


class Mem(Resource):
    def __init__(self, mem: float):
        self.mem = mem
        self.remaining = mem

    def satisfy(self, mem: float) -> bool:
        return self.remaining >= mem

    def alloc(self, mem: float) -> float:
        assert self.satisfy(mem), 'Mem resource not available'
        self.remaining -= mem
        return mem

    def dealloc(self, mem: float):
        assert self.remaining + mem <= self.mem, \
            'Error dealloc resources: remaining more than original'
        self.remaining += mem

    def utilization(self) -> float:
        return (self.mem - self.remaining) / self.mem


Gpus = List[float]


class GpuSet(Resource):
    def __init__(self, gpus: List[float]):
        """ Gpu resource is modeled as list of gpu memory of all gpus on a node.
        """
        self.gpus = gpus
        self.remaining = copy.deepcopy(gpus)

    def __repr__(self):
        return '<GPU set: max: {}, remain: {}>'.format(self.gpus, self.remaining)

    def satisfy(self, request: Gpus) -> bool:
        """ gpu requests are modeled as a list of memory requested for each gpu,
        assuming allocations are all from distinct gpus
        """
        # get the sorted remaining and requested memory, and see if all requests
        # can be satisfied
        availables = sorted(self.remaining, reverse=True)
        requests = sorted(request, reverse=True)

        for i, req in enumerate(requests):
            if availables[i] < req:
                return False

        return True

    def alloc(self, request: Gpus):
        # print(' allocating node gpu resources %s with %s' %
        #       (self.remaining, request))

        for i, req in enumerate(request):
            assert self.remaining[i] >= req, 'Gpu resource not available'
            self.remaining[i] -= req

    def dealloc(self, request: Gpus):
        for i, req in enumerate(request):
            self.remaining[i] += req
            assert self.remaining[i] <= self.gpus[i], \
                'Error dealloc resources: remaining more than original'

    def utilization(self) -> float:
        return (sum(self.gpus) - sum(self.remaining)) / sum(self.gpus)


class Node:
    def __init__(self, env, node_id: int, resources: ResourcesMapType):
        self.env: Optional[Environment] = env
        self.node_id: int = node_id
        self.resources: ResourcesMapType = resources

        # gather statistics about the node
        self.tasks = 0
        self.records = DataFrame(
            columns=['cpu-util', 'mem-util', 'gpu-util', 'tasks'])

        # self.records: Dict[str, List[tuple]] = defaultdict(lambda: [(0, 0.0)])

    def __repr__(self):
        return 'Node {} with resources {}'.format(self.node_id, self.resources)

    def satisfy(self, resources: ResourcesMapType) -> bool:
        success = all(self.resources[name].satisfy(resource)
                      for name, resource in resources.items())
        # print('%d: Try to allocate %s with %s %s' %
        #       (self.env.now, resources['gpus'], self.resources['gpus'], success))

        return all(self.resources[name].satisfy(resource)
                   for name, resource in resources.items())

    def alloc(self, resources: ResourcesMapType) -> ResourcesMapType:
        ret: ResourcesMapType = {}

        # print('%d: Allocated %s with %s' %
        #       (self.env.now, resources['gpus'], self.resources['gpus']))

        for name, resource in resources.items():
            ret[name] = self.resources[name].alloc(resource)

        self.tasks += 1

        self.record({
            'task': self.tasks,
            'gpu-util': self.resources['gpus'] .utilization(),
        })

        return ret

    def dealloc(self, resources: ResourcesMapType):
        for name, resource in resources.items():
            self.resources[name].dealloc(resource)

        self.tasks -= 1

        self.record({
            'task': self.tasks,
            'gpu-util': self.resources['gpus'] .utilization(),
        })

    def record(self, row: Dict):
        assert self.env is not None, \
            'Environment not initialized when recording'

        self.records = self.records.append(
            DataFrame(row, index=[self.env.now]))

        # """ if self.env.now != 0:
        #     self.records[key].append((self.env.now-1, self.records[key])) """

        # if self.env.now != 0 and len(self.records[key]) > 0:
        #     self.records[key].append(
        #         (self.env.now - 1, self.records[key][-1][1]))

        # self.records[key].append((self.env.now, value))
