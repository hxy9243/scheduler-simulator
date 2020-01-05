import simpy
import sys

sys.path.append('..')  # noqa

from core.workload import UnifiedRandomWorkload
from core.simulator import Simulator
from core.node import Node, Cpu, Mem, Gpu
from core.scheduler import BasicScheduler, RandomDispatcher


sim = Simulator({})

sim.add_workload(UnifiedRandomWorkload())

nodes = [Node([Gpu()])]
for node in nodes:
    sim.add_node(node)

dispatcher = RandomDispatcher([BasicScheduler([node])])
sim.add_dispatcher(dispatcher)

# sim.add_scheduler(BasicScheduler([node]))

sim.run()

print(sim.inqueue.items)

sim.print_logs()
