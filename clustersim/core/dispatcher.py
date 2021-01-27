from typing import List, Optional
import random

from simpy import Environment

from clustersim.core.resources import Node
from clustersim.core.workload import Workload, get_workload
from clustersim.core.scheduler import Scheduler, get_scheduler


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
