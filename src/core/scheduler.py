from typing import List, Dict, Optional
import random
from collections import defaultdict

from .resources import Node, Resource


class Scheduler:
    def __init__(self):
        self.queue = []
        self.simulator = None

    def schedule(self, task):
        raise NotImplementedError('Not implemented')

    def run(self):
        raise NotImplementedError('Not implemented')


class BasicScheduler(Scheduler):
    def __init__(self):
        Scheduler.__init__(self)

        self.records = defaultdict(list)

    def add(self, job):
        self.queue.append(job)

    def satisfy(self, resources: Dict[str, Resource]) -> bool:
        assert isinstance(resources, dict), \
            'Resource should be described as dictionary'

        return any(node.satisfy(resources)
                   for node in self.simulator.nodes)

    def find_node(self, resources) -> Optional[Node]:
        for node in self.simulator.nodes:
            if node.satisfy(resources):
                return node
        return None

    def schedule(self, job, node, alloc):
        self.simulator.log('Job {} scheduled'.format(job))

        job.scheduled_time = self.simulator.env.now
        self.simulator.env.process(job.run(self, node, alloc))

    def run(self):
        assert self.simulator is not None, 'Scheduler simulator is none'

        while True:
            for i, job in enumerate(self.queue):
                node = self.find_node(job.resources)
                if node:
                    yield self.simulator.env.timeout(1)
                    alloc = node.alloc(job.resources)

                    self.queue.pop(i)
                    self.schedule(job, node, alloc)

            yield self.simulator.env.timeout(1)
            self.simulator.env.step()

    def record(self, key: str, value: float):
        #print((self.simulator.env.now, self.resources['gpu']))

        self.records[key].append((self.simulator.env.now, value))


class Dispatcher:
    def __init__(self, schedulers: List[Scheduler]):
        self.simulator: Optional['Simulator'] = None
        self.schedulers = schedulers

    def add_scheduler(self, scheduler):
        scheduler.simulator = self.simulator
        self.schedulers.append(scheduler)

    def dispatch(self, job):
        raise NotImplementedError('Not implemented')


class RandomDispatcher(Dispatcher):
    def __init__(self, schedulers):
        Dispatcher.__init__(self, schedulers)

    def dispatch(self, job):
        # dispatch the job to scheduler
        scheduler = random.choice(self.schedulers)

        scheduler.add(job)
