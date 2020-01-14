from typing import List, Dict, Tuple, Union, Optional

from enum import Enum, auto
import random

from .resources import Resource, Node


class Workload:
    """ Define a type of workload, that generates a category of jobs
    """

    def __init__(self):
        self.simulator: Optional['Simulator'] = None

    def generate(self) -> Union['Task', 'Job']:
        raise NotImplementedError('Not implemented')

    def run(self):
        raise NotImplementedError('Not implemented')
        

class UnifiedRandomWorkload(Workload):
    """ A basic Random Workload that generates jobs
    """

    def __init__(self, income_range: Tuple[int, int], task_timerange: Tuple[int, int],
                 resources: Dict[str, 'Resource']):
        Workload.__init__(self)

        self.income_range = income_range
        self.task_timerange = task_timerange
        self.resources = resources
        self.simulator: Optional['Simulator'] = None

        self.jobid = 1

    def generate(self) -> Union['Task', 'Job']:
        assert self.simulator, 'Simulator of workload not initialized'

        task = Task(self.simulator, self.jobid,
                    self.jobid,
                    random.uniform(*self.task_timerange),
                    resources=self.resources)
        self.jobid += 1
        return task

    def run(self):
        assert self.simulator is not None, 'No simulator specified'

        while True:
            yield self.simulator.env.timeout(random.uniform(*self.income_range))
            job = self.generate()

            self.simulator.log('Job {} arriving'.format(job))
            self.simulator.dispatch(job)


class Job:
    """ Define a job, which consists of multiple tasks
    """

    def __init__(self, jobid: int, tasks: List['Task']):
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

    def __init__(self, simulator: 'Simulator', jobid: int, taskid: int,
                 task_runtime: float, resources: Dict[str, 'Resource']):
        self.simulator = simulator
        self.jobid = jobid
        self.taskid = taskid
        self.task_runtime = task_runtime
        self.resources = resources
        self.allocation: Optional[Resource] = None

        self.created_time = simulator.env.now
        self.scheduled_time: float = 0.0
        self.finished_time: float = 0.0

        self.node: Optional['Node'] = None
        self.status = TaskStatus.INIT

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
        self.status = TaskStatus.RUNNING

        # run the actual task
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
        self.status = TaskStatus.FINISHED
