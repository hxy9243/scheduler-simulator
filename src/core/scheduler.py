import random


class BaseScheduler:
    def __init__(self, nodes):
        self.nodes = nodes
        self.queue = []
        self.simulator = None

    def schedule(self, task):
        raise NotImplementedError('Not implemented')

    def run(self):
        raise NotImplementedError('Not implemented')


class BasicScheduler(BaseScheduler):
    def __init__(self, nodes):
        BaseScheduler.__init__(self, nodes)

    def schedule(self, job):
        pass

    def run(self):
        while True:
            if self.queue:
                yield self.simulator.env.timeout(1)
                job = self.queue.pop()

                self.simulator.log('Job {} scheduled'.format(job))
                self.simulator.env.process(job.run(self))
            else:
                self.simulator.env.step()


class BaseDispatcher:
    def __init__(self, schedulers):
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

        scheduler.queue.append(job)
