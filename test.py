from typing import List
from time import sleep
from random import randint
from kad_config import KadConfig
from kad_types import NodeId
from node import Node
from logger import Logger
from lock import ExtLock

ExtLock.init("locks.txt", enabled=False)
Logger.init("kad.txt")
conf: KadConfig = KadConfig(id_length=8, alpha=3, k=3)
Logger.log_config(conf)

origin_id: NodeId = NodeId(0)
origin = Node(NodeId(origin_id), conf)
origin.run()

nodes: List[Node] = [Node(NodeId(i), conf, origin=origin_id) for i in range(1, 10)]

for node in nodes:
    node.run()
    sleep(randint(50, 300) / 1000)

for node in nodes:
    node.join(timeout=1)

for node in nodes:
    node.terminate()
origin.terminate()

print("Done")
