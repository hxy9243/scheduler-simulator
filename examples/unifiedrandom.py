from clustersim.core.workload import Task, Job, UnifiedRandomWorkload
from clustersim.core.resources import Cpu, Mem, Gpu, Node
from clustersim.core.scheduler import BasicScheduler, RandomDispatcher
from clustersim.core.simulator import Simulator

from matplotlib import pyplot as plt
from simpy import Environment

env = Environment()
nodes = [Node(env, 1, {'gpu': Gpu([1, 1, 1, 1])})]
workload = UnifiedRandomWorkload(
    env,
    income_range=(4, 12), task_timerange=(16, 36),
    resources={'gpu': Gpu([0.5, 0.5])})
dispatcher = RandomDispatcher(env, workload, [BasicScheduler(env, nodes)])
sim = Simulator(
    env,
    workloads=[workload], nodes=nodes, dispatcher=dispatcher,
    configs={})

sim.run(until=20000)

# Print out the node statistics
node_stats = sim.nodes[0].records
index = [t[0] for t in node_stats['gpu-util']]
value = [t[1] for t in node_stats['gpu-util']]

plt.plot(index, value)
plt.title('gpu utilization')
plt.show()

# Print out the task statistics
s_rec = sim.dispatcher.schedulers[0].records
task_runtime = [v[1] for v in s_rec['task_runtime']]
task_waittime = [v[1] for v in s_rec['task_waittime']]
task_total = [v[1] for v in s_rec['task_total']]

fig, sub = plt.subplots(2, 2)

sub[0][0].hist(task_runtime)
sub[0][0].set_title('run time')

sub[0][1].hist(task_waittime)
sub[0][1].set_title('wait time')

sub[1][0].hist(task_total)
sub[1][0].set_title('total')

plt.show()
