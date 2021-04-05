from config import Config
from bucket import Bucket
from routing_table import RoutingTable
from peer import Peer

conf: Config = Config(list_size=5, id_length=8)
b = Bucket(2)
b.add(Peer(10, 10))
b.add(Peer(20, 2))
print(b)
print(b.last_seen())

# tr: RoutingTable = RoutingTable(0, conf)
# p1: Peer = Peer(10)
# tr.add_peer(p1)

