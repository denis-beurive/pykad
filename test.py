from typing import List
from kad_config import KadConfig
from bucket import Bucket
from routing_table import RoutingTable
from peer_data import PeerData
from kad_types import PeerId
from peer import Peer
from time import sleep
from queue_manager import QueueManager


conf: KadConfig = KadConfig(list_size=5, id_length=8, alpha=3)
queue_manager = QueueManager()

origin = Peer(PeerId(0), conf, queue_manager)
peer1 = Peer(PeerId(1), conf, queue_manager, origin=origin.data)
peer2 = Peer(PeerId(2), conf, queue_manager, origin=origin.data)
peer3 = Peer(PeerId(3), conf, queue_manager, origin=origin.data)

peers: List[Peer] = [origin, peer1, peer2, peer3]

for peer in peers:
    peer.run()
    sleep(1)

for peer in peers:
    peer.join(timeout=4)

for peer in peers:
    peer.terminate()

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
