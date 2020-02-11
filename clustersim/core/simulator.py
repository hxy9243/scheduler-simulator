from typing import List, Dict, Any, Union

from clustersim.core.scheduler import Dispatcher, Scheduler, RandomDispatcher
from clustersim.core.resources import Node
from clustersim.core.workload import Workload, Task, Job

import simpy


def log():
    pass


class Simulator:
    def __init__(self, env: simpy.Environment,
                 workloads: List[Workload], nodes: List[Node],
                 dispatcher: Dispatcher, configs: Dict[str, Any]):
        self.env: simpy.Environment = env
        self.inqueue: simpy.Store = simpy.Store(self.env)

        self.nodes = nodes
        self.workloads = workloads
        self.dispatcher = dispatcher

        self.configs = configs

    def log(self, msg):
        self.logs.append((self.env.now, msg))

    def print_logs(self):
        for log in self.logs:
            print(log[0], log[1])

    def run(self, until=200):
        for workload in self.workloads:
            self.env.process(workload.run())

        self.env.process(self.dispatcher.run())

        self.env.run(until=until)
