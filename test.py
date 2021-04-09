from typing import List
from kad_config import KadConfig
from bucket import Bucket
from routing_table import RoutingTable
from node_data import NodeData
from kad_types import NodeId
from node import Node
from time import sleep
from queue_manager import QueueManager


conf: KadConfig = KadConfig(list_size=5, id_length=8, alpha=3)
queue_manager = QueueManager()

print("Create the nodes")
origin_id: NodeId = NodeId(0)
print(".")
origin = Node(NodeId(origin_id), conf, queue_manager)
print(".")
node1 = Node(NodeId(1), conf, queue_manager, origin=origin_id)
print(".")
node2 = Node(NodeId(2), conf, queue_manager, origin=origin_id)
print(".")
node3 = Node(NodeId(3), conf, queue_manager, origin=origin_id)
print(".")


print("Start the nodes")
nodes: List[Node] = [origin, node1, node2, node3]
for node in nodes:
    node.run()
    sleep(1)

for node in nodes:
    node.join(timeout=4)

for node in nodes:
    node.terminate()

print("Done")







# b = Bucket(2)
# b.add(Peer(10, 10))
# b.add(Peer(20, 2))
# print(b)
# print(b.most_recent())

# local_peer_id: PeerId = PeerId(5)
# tr: RoutingTable = RoutingTable(local_peer_id, conf)
# for i in range(255):
#     if i == local_peer_id:
#         continue
#     tr.add_peer(PeerData(PeerId(i), i))
# print(tr)
