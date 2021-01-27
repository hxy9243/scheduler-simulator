from clustersim.core.simulator import Simulator
from clustersim.core.resources import GpuSet

from matplotlib import pyplot as plt


sim = Simulator()

sim.add_node({'gpus': GpuSet([1, 1, 1, 1])})
dispatcher = sim.add_dispatcher('random')

dispatcher.add_workload('unified_random',
                        income_range=(4, 12), tasktime_range=(16, 36),
                        resources={'gpus': [0.5, 0.5]})
dispatcher.add_scheduler('basic', sim.nodes)

sim.run(until=2000)

# Print out the node statistics
node_stats = sim.nodes[0].records
print(node_stats.loc[:, ('gpu-util', 'task')].head(20))
print('gpu util mean: ', node_stats['gpu-util'].mean(axis='index'))
