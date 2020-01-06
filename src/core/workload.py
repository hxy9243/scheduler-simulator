from enum import Enum, auto
import random


class BaseWorkload:
    """ Define a type of workload, that generates a category of jobs
    """

    def __init__(self):
        self.simulator = None

    def generate(self):
        raise NotImplementedError('Not implemented')

    def run(self):
        raise NotImplementedError('Not implemented')


class UnifiedRandomWorkload(BaseWorkload):
    """ A basic Random Workload that generates jobs
    """

    def __init__(self, income_range, task_timerange, resources):
        BaseWorkload.__init__(self)

        self.income_range = income_range
        self.task_timerange = task_timerange
        self.resources = resources

        self.jobid = 1

    def generate(self):
        task = Task(self.simulator, self.jobid,
                    self.jobid,
                    random.randrange(*self.task_timerange),
                    resources=self.resources)
        self.jobid += 1
        return task

    def run(self):
        assert self.simulator is not None, 'No simulator specified'

        while True:
            yield self.simulator.env.timeout(*self.income_range)
            job = self.generate()

            self.simulator.log('Job {} arriving'.format(job))
            self.simulator.dispatch(job)


class Job:
    """ Define a job, which consists of multiple tasks
    """

    def __init__(self, jobid, tasks):
        self.jobid = jobid
        self.tasks = tasks


class TaskStatus(Enum):
    INIT = auto()
    RUNNING = auto()
    FINISHED = auto()


class Task:
    """ Task define one single task, that requires resources from one or multiple
    nodes, and execute a certain amount of time
    """

    def __init__(self, simulator, jobid, taskid, task_runtime, resources):
        self.simulator = simulator
        self.jobid = jobid
        self.taskid = taskid
        self.task_runtime = task_runtime
        self.resources = resources
        self.allocation = None

        self.created_time = simulator.env.now
        self.scheduled_time = None
        self.finished_time = None

        self.scheduler = None
        self.node = None
        # self.status = TaskStatus.INIT

    def __repr__(self):
        return '<Task {}>'.format(self.taskid)

    def assign(self, node):
        pass

    def run(self, scheduler, node, alloc):
        self.scheduler = scheduler
        self.node = node
        self.allocation = alloc

        assert self.simulator is not None, \
            'Task {} simulator is none'.format(self)
        assert self.scheduler is not None, \
            'Task {} scheduler is none'.format(self)

        self.scheduled_time = self.simulator.env.now
        yield self.simulator.env.timeout(self.task_runtime)
        self.finished_time = self.simulator.env.now

        assert self.finished_time - self.created_time >= self.task_runtime

        # record statistics
        self.scheduler.record('task_runtime', self.task_runtime)
        self.scheduler.record(
            'task_waittime', self.scheduled_time - self.created_time)
        self.scheduler.record(
            'task_total', self.finished_time - self.created_time)

        self.simulator.log('Task {} finished'.format(self))

        self.node.dealloc(self.allocation)
