from config import Config
from bucket import Bucket
from routing_table import RoutingTable
from peer import Peer
from kad_types import PeerId

conf: Config = Config(list_size=5, id_length=8, alpha=3)
# b = Bucket(2)
# b.add(Peer(10, 10))
# b.add(Peer(20, 2))
# print(b)
# print(b.most_recent())

local_peer_id: PeerId = PeerId(5)
tr: RoutingTable = RoutingTable(local_peer_id, conf)
for i in range(255):
    if i == local_peer_id:
        continue
    tr.add_peer(Peer(PeerId(i), i))
print(tr)
