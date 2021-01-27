from clustersim.core.simulator import Simulator
from clustersim.core.workload import ClosedWorkload
from clustersim.core.resources import Cpu, Mem, Gpus, GpuSet, Node
import clustersim.core.scheduler

from matplotlib import pyplot as plt

sim = Simulator()

sim.add_node({'gpus': GpuSet([1, 1, 1, 1])})
dispatcher = sim.add_dispatcher('random')

for _ in range(4):
    dispatcher.add_workload('closed_random',
                            income_range=(0, 0), tasktime_range=(10, 100),
                            resources={'gpus': [0.2, 0.7]})
dispatcher.add_scheduler('basic', sim.nodes, scheme='worst_fit')

sim.run(until=2000)

node_stats = sim.nodes[0].records
print(node_stats.head())
print('gpu util mean: ', node_stats['gpu-util'].mean(axis='index'))
