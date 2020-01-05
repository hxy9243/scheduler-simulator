import simpy
from .scheduler import RandomDispatcher


def Log():
    def __init__(self):
        pass


class Logging():
    def __init__(self):
        pass


class Simulator:
    def __init__(self, config):
        self.env = simpy.Environment()
        self.logs = []
        self.inqueue = simpy.Store(self.env)

        self.workloads = []
        self.nodes = []
        self.schedulers = []
        self.dispatcher = None

    def log(self, msg):
        self.logs.append((self.env.now, msg))

    def print_logs(self):
        for log in self.logs:
            print(log[0], log[1])

    def add_workload(self, workload):
        workload.simulator = self
        self.workloads.append(workload)

    def add_node(self, node):
        node.simulator = self
        self.nodes.append(node)

    def add_scheduler(self, scheduler):
        scheduler.simulator = self
        self.schedulers.append(scheduler)

    def add_dispatcher(self, dispatcher):
        dispatcher.simulator = self
        self.dispatcher = dispatcher
        for scheduler in self.dispatcher.schedulers:
            scheduler.simulator = self

    def dispatch(self, job):
        self.dispatcher.dispatch(job)
        # def dispatch_schedulers(self):
        #     while True:
        #         if

    def run(self, until=200):
        for workload in self.workloads:
            self.env.process(workload.run())

        for scheduler in self.dispatcher.schedulers:
            self.env.process(scheduler.run())

        self.env.run(until=until)
