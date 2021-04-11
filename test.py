from typing import List
from kad_config import KadConfig
from kad_types import NodeId
from node import Node
from time import sleep
from logger import Logger


Logger.init("kad.txt")
conf: KadConfig = KadConfig(id_length=8, alpha=3, k=3)

origin_id: NodeId = NodeId(0)
origin = Node(NodeId(origin_id), conf)
origin.run()

nodes: List[Node] = [Node(NodeId(i), conf, origin=origin_id) for i in range(1, 10)]

for node in nodes:
    node.run()

for node in nodes:
    node.join(timeout=10)

for node in nodes:
    node.terminate()
origin.terminate()

print("Done")
