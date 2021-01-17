from typing import List, Dict, Optional, Union
import random
from collections import defaultdict

from simpy import Environment

from clustersim.core.resources import Node, Resource, ResourcesMapType
from clustersim.core.workload import Workload, Work, get_workload


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
    def __init__(self, env: Environment, nodes: List[Node]):
        Scheduler.__init__(self, env, nodes)

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

    def schedule(self, work: Work, node: Node, alloc: ResourcesMapType):
        # self.simulator.log('Job {} scheduled'.format(job))

        work.scheduled_time = self.env.now
        self.env.process(work.run(self.records, node, alloc))

    def run(self):
        assert self.env is not None, 'Scheduler environment is none'

        while True:
            for i, job in enumerate(self.queue):
                node = self.find_node(job.resources)
                if node:
                    yield self.env.timeout(1)
                    alloc = node.alloc(job.resources)

                    self.queue.pop(i)
                    self.schedule(job, node, alloc)

            yield self.env.timeout(1)

    def record(self, key: str, value: float):
        self.records[key].append((self.env.now, value))


def get_scheduler(env: Environment, schedulerType: str, nodes: List[Node]) -> Scheduler:
    if schedulerType == 'basic':
        return BasicScheduler(env, nodes)


class Dispatcher:
    def __init__(self, env: Environment):
        self.env: Optional[Environment] = env
        self.workloads: List[Workload] = []
        self.schedulers: List[Scheduler] = []

    def add_workload(self, workloadType: str, **args) -> Workload:
        workload = get_workload(self.env, workloadType, **args)

        self.workloads.append(workload)
        return workload

    def add_scheduler(self, schedulerType: str, nodes: List[Node]) -> Scheduler:
        scheduler = get_scheduler(self.env, schedulerType, nodes)

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
