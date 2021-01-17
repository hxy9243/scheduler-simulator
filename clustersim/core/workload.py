"""
workload module defines the work to be scheduled and executed on
the target machine:

- Workload: the workload generator that keep producing jobs/tasks
  with specified configurations.
- Job: a list of tasks.
- Task: a single unit of scheduling, a task occupies resource for 
  the length of its lifetime on a node, or several nodes.
"""

from typing import List, Dict, Tuple, Union, Optional

from enum import Enum, auto
import random
from collections import defaultdict

from simpy import Environment
from clustersim.core.resources import Resource, Node, ResourcesMapType


class Workload:
    """ Define a type of workload, that generates a category of jobs
    """

    def __init__(self, env: Environment):
        self.env: Optional[Environment] = env

    def generate(self) -> Union['Task', 'Job']:
        raise NotImplementedError('Not implemented')

    def finish_work(self, id: int):
        raise NotImplementedError('Not implemented')

    def run(self):
        raise NotImplementedError('Not implemented')


class UnifiedRandomWorkload(Workload):
    """ A basic Random Workload that generates jobs
    """

    def __init__(self, env: Environment,
                 income_range: Tuple[int, int], tasktime_range: Tuple[int, int],
                 resources: ResourcesMapType):
        Workload.__init__(self, env)

        self.income_range = income_range
        self.tasktime_range = tasktime_range
        self.resources = resources
        self.queue: List[Union[Task, Job]] = []

        self.jobid = 1

    def generate(self) -> Union['Task', 'Job']:
        assert self.env, 'Environment of workload not initialized'

        task = Task(self, self.jobid,
                    self.jobid,
                    random.uniform(*self.tasktime_range),
                    resources=self.resources)
        self.jobid += 1
        return task

    def finish_work(self, id: int):
        return

    def run(self):
        assert self.env is not None, 'No environment specified'

        while True:
            yield self.env.timeout(random.uniform(*self.income_range))
            job = self.generate()

            self.queue.append(job)


class ClosedWorkload(Workload):
    """ generate one job when there's one finished
    """

    def __init__(self, env: Environment,
                 income_range: Tuple[int, int], tasktime_range: Tuple[int, int],
                 resources: ResourcesMapType):
        Workload.__init__(self, env)

        self.income_range = income_range
        self.tasktime_range = tasktime_range
        self.resources = resources
        self.queue: List[Union[Task, Job]] = []

        self.jobid = 1
        self.task_event = None

    def generate(self) -> Union['Task']:
        task = Task(self, self.jobid, self.jobid,
                    random.uniform(*self.tasktime_range), resources=self.resources)
        self.jobid += 1

        return task

    def finish_work(self, taskid: int):
        self.task_event.succeed()
        self.task_event = self.env.event()

    def run(self):
        assert self.env is not None, 'No environment specified'

        print('Start generating work')
        self.task_event = self.env.event()

        while True:
            job = self.generate()
            self.queue.append(job)

            yield self.task_event
            # self.env.step()


def get_workload(env: Environment, workloadType: str, **args) -> Workload:
    if workloadType == 'unified_random':
        return UnifiedRandomWorkload(env, **args)
    elif workloadType == 'closed_random':
        return ClosedWorkload(env, **args)


class WorkStatus(Enum):
    INIT = auto()
    RUNNING = auto()
    FINISHED = auto()


class Work:
    status = WorkStatus.INIT

    queued_time: float = 0.0
    scheduled_time: float = 0.0
    finished_time: float = 0.0

    def run(self, records: Dict, node: Node, alloc: ResourcesMapType):
        raise NotImplementedError('Not implemented')


class Job(Work):
    """ Define a job, which consists of multiple tasks
    """

    def __init__(self, jobid: int, tasks: List['Task']):
        self.jobid = jobid
        self.tasks = tasks
        self.status = WorkStatus.INIT


class Task(Work):
    """ Task define one single task, that requires resources from one or multiple
    nodes, and execute a certain amount of time
    """

    def __init__(self, workload: Workload,
                 jobid: int, taskid: int,
                 task_runtime: float,
                 resources: ResourcesMapType):

        self.workload: Workload = workload
        self.env: Optional[Environment] = workload.env
        self.jobid: int = jobid
        self.taskid: int = taskid
        self.task_runtime: float = task_runtime
        self.resources: Dict = resources
        self.allocation: ResourcesMapType = {}

        self.queued_time: float = workload.env.now
        self.scheduled_time: float = 0.0
        self.finished_time: float = 0.0

        self.node: Optional[Node] = None
        self.status = WorkStatus.INIT

        self.record: defaultdict = defaultdict(int)

    def __repr__(self):
        return '<Task {}>'.format(self.taskid)

    def assign(self, node: Node):
        pass

    def run(self, records: Dict, node: Node, alloc: ResourcesMapType):
        self.node = node
        self.allocation = alloc

        assert self.env is not None, \
            'Task {} environment is none'.format(self)

        self.scheduled_time = self.env.now
        self.status = WorkStatus.RUNNING

        # run the actual task
        yield self.env.timeout(self.task_runtime)
        now = self.env.now
        self.finished_time = now

        self.workload.finish_work(self.taskid)

        assert self.finished_time - self.queued_time >= self.task_runtime, \
            'Tasks runs doesn\'t run for enough time'

        # record statistics
        records['task_runtime'].append((now, self.task_runtime))
        records['task_waittime'].append(
            (now, self.scheduled_time - self.queued_time))
        records['task_total'].append(
            (now, self.finished_time - self.queued_time))

        # self.simulator.log('Task {} finished'.format(self))
        self.node.dealloc(self.allocation)
        self.status = WorkStatus.FINISHED
