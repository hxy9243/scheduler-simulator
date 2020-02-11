from typing import List, Dict, Optional, Union
import random
from collections import defaultdict

from simpy import Environment

from clustersim.core.resources import Node, Resource, AllocType
from clustersim.core.workload import Workload, Work


class Scheduler:
    def __init__(self, env: Environment, nodes: List[Node]):
        self.queue: List[Work] = []
        self.env: Environment = env
        self.nodes: List[Node] = nodes
        self.records: defaultdict = defaultdict(list)

    def schedule(self, work: Work, node: Node, alloc: AllocType):
        raise NotImplementedError('Not implemented')

    def run(self):
        raise NotImplementedError('Not implemented')


class BasicScheduler(Scheduler):
    def __init__(self, env: Environment, nodes: List[Node]):
        Scheduler.__init__(self, env, nodes)

    def add(self, job):
        self.queue.append(job)

    def satisfy(self, resources: AllocType) -> bool:
        assert isinstance(resources, dict), \
            'Resource should be described as dictionary'

        return any(node.satisfy(resources) for node in self.nodes)

    def find_node(self, resources) -> Optional[Node]:
        for node in self.nodes:
            if node.satisfy(resources):
                return node
        return None

    def schedule(self, work: Work, node: Node, alloc: AllocType):
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
            self.env.step()

    def record(self, key: str, value: float):
        self.records[key].append((self.env.now, value))


class Dispatcher:
    def __init__(self, env: Environment, workload: Workload, schedulers: List[Scheduler]):
        self.env: Optional[Environment] = env
        self.workload = workload
        self.schedulers = schedulers

    def add_scheduler(self, scheduler):
        self.schedulers.append(scheduler)

    def dispatch(self, job):
        raise NotImplementedError('Not implemented')


class RandomDispatcher(Dispatcher):
    def __init__(self, env: Environment, workload: Workload, schedulers: List[Scheduler]):
        Dispatcher.__init__(self, env, workload, schedulers)

    def dispatch(self, job):
        """dispatch the job to scheduler"""
        scheduler = random.choice(self.schedulers)
        scheduler.add(job)

    def run(self):
        assert self.env is not None, 'No environment specified'

        while True:
            if self.workload.queue:
                self.dispatch(self.workload.queue.pop(0))

            yield self.env.timeout(0)
            self.env.step()
