from typing import List
from kad_config import KadConfig
from kad_types import NodeId
from node import Node
from time import sleep
from logger import Logger


Logger.init("kad.log")
conf: KadConfig = KadConfig(list_size=5, id_length=8, alpha=3, k=3)

origin_id: NodeId = NodeId(0)
origin = Node(NodeId(origin_id), conf)

nodes: List[Node] = [origin]
nodes.extend([Node(NodeId(i), conf, origin=origin_id) for i in range(1, 15)])

for node in nodes:
    node.run()
    sleep(1)

for node in nodes:
    node.join(timeout=4)

for node in nodes:
    node.terminate()

print("Done")
