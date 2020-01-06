import simpy
import sys

sys.path.append('..')  # noqa

from core.workload import UnifiedRandomWorkload
from core.simulator import Simulator
from core.resources import Cpu, Mem, Gpu, Node
from core.scheduler import BasicScheduler, RandomDispatcher


sim = Simulator({})

sim.add_workload(UnifiedRandomWorkload())

nodes = [
    Node(1, {'gpu': Gpu([1, 1, 1, 1])}),
]
for node in nodes:
    sim.add_node(node)

dispatcher = RandomDispatcher([
    BasicScheduler(nodes),
])
sim.add_dispatcher(dispatcher)
sim.run()

print(sim.inqueue.items)

sim.print_logs()
