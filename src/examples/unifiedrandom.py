import simpy
import sys

sys.path.append('..')  # noqa

from core.workload import UnifiedRandomWorkload
from core.simulator import Simulator
from core.resources import Cpu, Mem, Gpu, Node
from core.scheduler import BasicScheduler, RandomDispatcher

from matplotlib import pyplot as plt

sim = Simulator(
    workloads=[
        UnifiedRandomWorkload(income_range=(4, 16),
                              task_timerange=(2, 12),
                              resources={'gpu': Gpu([0.5, 0.5])}),
    ],
    nodes=[Node(1, {'gpu': Gpu([1, 1, 1, 1])})],
    dispatcher=RandomDispatcher([BasicScheduler()]),
    configs={},
)

sim.run(until=2000)

node_stats = sim.nodes[0].get_records()

index = [t[0] for t in node_stats['gpu-util']]
value = [t[1] for t in node_stats['gpu-util']]

plt.plot(index, value)
plt.title('gpu utilization')
plt.show()

print(sim.dispatcher.schedulers[0])

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
