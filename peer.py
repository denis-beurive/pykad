from typing import Optional
from threading import Thread
from time import sleep
from queue import Queue
from kad_types import PeerId
from peer_data import PeerData
from config import Config
from routing_table import RoutingTable


class Peer:

    def __init__(self, peer_id: PeerId, config: Config, origin: Optional[PeerData] = None):
        self.__peer_id: PeerId = peer_id
        self.__is_bootstrap_peer: bool = origin is None
        self.__origin: Optional[PeerData] = origin
        self.__routing_table: RoutingTable = RoutingTable(peer_id, config)
        self.__input_queue: Queue = Queue()
        self.__listener: Thread = Thread(target=self.__listener_implementation, args=[])
        self.__core: Thread = Thread(target=self.__core_implementation, args=[])

    @property
    def input_queue(self) -> Queue:
        return self.__input_queue

    @input_queue.setter
    def input_queue(self, value: Queue) -> None:
        self.__input_queue = value

    def __listener_implementation(self) -> None:
        sleep(2)
        print("[listener] local peer ID: {0:d}".format(self.__peer_id))

    def __core_implementation(self) -> None:
        sleep(3)
        print("[core] local peer ID: {0:d}".format(self.__peer_id))

    @property
    def data(self) -> PeerData:
        return PeerData(identifier=self.__peer_id, queue=self.input_queue)

    def run(self) -> None:
        self.__listener.start()
        self.__core.start()

    def join(self) -> None:
        self.__listener.join()
        self.__core.join()


conf: Config = Config(list_size=5, id_length=8, alpha=3)

origin = Peer(PeerId(0), conf)
peer1 = Peer(PeerId(10), conf, origin=origin.data)

origin.run()
peer1.run()

origin.join()
peer1.join()
print("Done")
