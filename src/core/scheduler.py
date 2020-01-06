from typing import List, Dict

import random
from .resources import Node, Resource
from .workload import Task


class BaseScheduler:
    def __init__(self):
        self.queue = []
        self.simulator = None

    def schedule(self, task):
        raise NotImplementedError('Not implemented')

    def run(self):
        raise NotImplementedError('Not implemented')


class BasicScheduler(BaseScheduler):
    def __init__(self):
        BaseScheduler.__init__(self)

    def add(self, job):
        self.queue.append(job)

    def satisfy(self, resources: Dict[str, Resource]) -> bool:
        assert isinstance(resources, dict), \
            'Resource should be described as dictionary'

        return any(node.satisfy(resources)
                   for node in self.simulator.nodes)

    def find_node(self, resources) -> Node:
        for node in self.simulator.nodes:
            if node.satisfy(resources):
                return node
        return None

    def schedule(self, job, node, alloc):
        self.simulator.log('Job {} scheduled'.format(job))
        self.simulator.env.process(job.run(self, node, alloc))

    def run(self):
        assert self.simulator is not None, 'Scheduler simulator is none'

        while True:
            for i, job in enumerate(self.queue):
                node = self.find_node(job.resources)
                if node:
                    alloc = node.alloc(job.resources)

                    self.queue.pop(i)
                    self.schedule(job, node, alloc)

            yield self.simulator.env.timeout(0)
            self.simulator.env.step()


class BaseDispatcher:
    def __init__(self, schedulers: List[BaseScheduler]):
        self.simulator = None
        self.schedulers = schedulers

    def add_scheduler(self, scheduler):
        scheduler.simulator = self.simulator
        self.schedulers.append(scheduler)


class RandomDispatcher(BaseDispatcher):
    def __init__(self, schedulers):
        BaseDispatcher.__init__(self, schedulers)

    def dispatch(self, job):
        # dispatch the job to scheduler
        scheduler = random.choice(self.schedulers)

        scheduler.add(job)
