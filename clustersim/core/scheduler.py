from typing import List, Optional, Set
import random
from collections import defaultdict
import copy

from simpy import Environment

from clustersim.core.resources import GpuSet, Gpus, Node, ResourcesMapType
from clustersim.core.workload import Workload, Work, Task, get_workload


class Scheduler:
    def __init__(self, env: Environment, nodes: List[Node]):
        self.queue: List[Work] = []
        self.env: Environment = env
        self.nodes: List[Node] = nodes
        self.records: defaultdict = defaultdict(list)

    def schedule(self, work: Work, node: Node, alloc: ResourcesMapType):
        raise NotImplementedError('Not implemented')

    def run(self):
        raise NotImplementedError('Not implemented')


class BasicScheduler(Scheduler):
    def __init__(self,
                 env: Environment,
                 nodes: List[Node],
                 scheme: str = 'worst_fit',
                 ):
        Scheduler.__init__(self, env, nodes)
        self.scheme = scheme

    def add(self, job):
        self.queue.append(job)

    def satisfy(self, resources: ResourcesMapType) -> bool:
        assert isinstance(resources, dict), \
            'Resource should be described as dictionary'

        return any(node.satisfy(resources) for node in self.nodes)

    def find_node(self, resources) -> Optional[Node]:
        for node in self.nodes:
            if node.satisfy(resources):
                return node
        return None

    def start_work(self, work: Work, node: Node, alloc: ResourcesMapType):
        # self.simulator.log('Job {} scheduled'.format(job))

        work.scheduled_time = self.env.now
        self.env.process(work.run(self.records, node, alloc))

    def schedule(self, task: Task, node: Node) -> ResourcesMapType:
        alloc: ResourcesMapType = dict()

        alloc['gpus'] = self.schedule_gpu(
            node.resources['gpus'], task.resources['gpus'])

        return alloc

    def schedule_gpu(self, node_gpus: GpuSet, request: Gpus) -> Gpus:
        remaining = enumerate(node_gpus.remaining)

        if self.scheme == 'worst_fit':
            availables = sorted(
                remaining,
                key=lambda x: x[1],
                reverse=True,
            )
        elif self.scheme == 'best_fit':
            availables = sorted(
                remaining,
                key=lambda x: x[1],
            )
        elif self.scheme == 'random':
            availables = random.sample(
                list(remaining), k=len(node_gpus.remaining))
        else:
            raise Exception('Unknown basic scheduling scheme %s' % self.scheme)

        requests = sorted(request, reverse=True)

        alloc = [0.0 for _ in node_gpus.gpus]
        alloced: Set[int] = set()

        for req in requests:
            found = False
            for i, gpu in availables:
                if i in alloced:
                    continue
                if req <= gpu:
                    alloc[i] = req
                    alloced.add(i)
                    found = True
                    break

            if not found:
                raise Exception('unable to satisfy gpu allocation')

        return alloc

    def run(self):
        assert self.env is not None, 'Scheduler environment is none'

        while True:
            for i, job in enumerate(self.queue):
                node = self.find_node(job.resources)
                if node:
                    yield self.env.timeout(1)

                    alloc = self.schedule(job, node)

                    # print('Allocated with node %d gpu: %s' %
                    #       (node.node_id, alloc))
                    node.alloc(alloc)

                    self.queue.pop(i)
                    self.start_work(job, node, alloc)

            yield self.env.timeout(1)

    def record(self, key: str, value: float):
        self.records[key].append((self.env.now, value))


def get_scheduler(env: Environment, schedulerType: str, nodes: List[Node], *args, **kwargs) -> Scheduler:
    if schedulerType == 'basic':
        return BasicScheduler(env, nodes, *args, **kwargs)


class Dispatcher:
    def __init__(self, env: Environment):
        self.env: Optional[Environment] = env
        self.workloads: List[Workload] = []
        self.schedulers: List[Scheduler] = []

    def add_workload(self, workloadType: str, **args) -> Workload:
        workload = get_workload(self.env, workloadType, **args)

        self.workloads.append(workload)
        return workload

    def add_scheduler(self, schedulerType: str, nodes: List[Node], *args, **kwargs) -> Scheduler:
        scheduler = get_scheduler(
            self.env, schedulerType, nodes, *args, **kwargs)

        self.schedulers.append(scheduler)
        return scheduler


def dispatch(self, job):
    raise NotImplementedError('Not implemented')


class SingleDispatcher(Dispatcher):
    def __init__(self, env: Environment):
        Dispatcher.__init__(self, env)

    def dispatch(self, job):
        """dispatch the job to scheduler"""
        scheduler = random.choice(self.schedulers)
        scheduler.add(job)

    def run(self):
        assert self.env is not None, 'No environment specified'

        for scheduler in self.schedulers:
            self.env.process(scheduler.run())

        while True:
            for workload in self.workloads:
                if workload.queue:
                    self.dispatch(workload.queue.pop(0))

                yield self.env.timeout(0)
                self.env.step()


def get_dispatcher(env: Environment, dispatcherType: str) -> Dispatcher:
    if dispatcherType == 'random':
        return SingleDispatcher(env)

    raise Exception(f'No dispatcher type: {dispatcherType}')
