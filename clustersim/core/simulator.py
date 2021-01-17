from typing import List, Dict, Any, Optional, Union

from .scheduler import get_dispatcher, Dispatcher
from .resources import Node, Resource, ResourcesMapType
from .workload import Workload, Task, Job

import simpy
from simpy import Environment


def log():
    pass


class Simulator:
    def __init__(self, configs: Dict[str, Any] = {}):
        """ workloads: List[Workload] = [],
        nodes: List[Node] = [],
        dispatcher: Optional[Dispatcher] = None,
        configs: Dict[str, Any] = {}): """

        self.env = Environment()
        self.inqueue: simpy.Store = simpy.Store(self.env)

        self.nodes: List[Node] = []
        self.workloads: List[Workload] = []
        self.dispatcher: Option[Dispatcher] = None
        self.configs: Dict[str, Any] = {}

    def add_node(self, resources: ResourcesMapType) -> Node:
        node = Node(self.env, len(self.nodes), resources)

        self.nodes.append(node)
        return node

    def add_dispatcher(self, dispatcherType: str) -> Dispatcher:
        dispatcher = get_dispatcher(self.env, dispatcherType)

        self.dispatcher = dispatcher
        return dispatcher

    def log(self, msg):
        self.logs.append((self.env.now, msg))

    def print_logs(self):
        for log in self.logs:
            print(log[0], log[1])

    def run(self, until=200):
        for workload in self.dispatcher.workloads:
            self.env.process(workload.run())

        self.env.process(self.dispatcher.run())

        self.env.run(until=until)
