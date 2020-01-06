import simpy
import sys

sys.path.append('..')  # noqa

from core.workload import UnifiedRandomWorkload
from core.simulator import Simulator
from core.resources import Cpu, Mem, Gpu, Node
from core.scheduler import BasicScheduler, RandomDispatcher


sim = Simulator(
    workloads=[UnifiedRandomWorkload()],
    nodes=[
        Node(1, {'gpu': Gpu([1, 1, 1, 1])}),
    ],
    dispatcher=RandomDispatcher([
        BasicScheduler(),
    ]),
    configs={},
)

sim.run()

sim.print_logs()
