from typing import List, Dict, Any, Union

from .scheduler import Dispatcher, Scheduler, RandomDispatcher
from .resources import Node
from .workload import Workload, Task, Job

import simpy


def Log():
    def __init__(self):
        pass


class Logging():
    def __init__(self):
        pass


class Simulator:
    def __init__(self, workloads: List['Workload'], nodes: List['Node'],
                 dispatcher: Dispatcher, configs: Dict[str, Any]):
        self.env = simpy.Environment()
        self.logs: List[Any] = []
        self.inqueue = simpy.Store(self.env)

        self.workloads: List['Workload'] = []
        self.nodes: List['Node'] = []

        for workload in workloads:
            self.add_workload(workload)

        for node in nodes:
            self.add_node(node)

        self.add_dispatcher(dispatcher)
        self.configs = configs

    def log(self, msg):
        self.logs.append((self.env.now, msg))

    def print_logs(self):
        for log in self.logs:
            print(log[0], log[1])

    def add_workload(self, workload: 'Workload'):
        workload.simulator = self
        self.workloads.append(workload)

    def add_node(self, node: 'Node'):
        node.simulator = self
        self.nodes.append(node)

    def add_dispatcher(self, dispatcher: Dispatcher):
        dispatcher.simulator = self
        self.dispatcher = dispatcher
        for scheduler in self.dispatcher.schedulers:
            scheduler.simulator = self

    def dispatch(self, job: Union['Job', 'Task']):
        self.dispatcher.dispatch(job)

    def run(self, until=200):
        for workload in self.workloads:
            self.env.process(workload.run())

        for scheduler in self.dispatcher.schedulers:
            self.env.process(scheduler.run())

        self.env.run(until=until)
