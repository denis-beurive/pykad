from typing import Optional, Dict, Callable
from threading import Thread
from time import sleep
from queue import Queue
from kad_types import PeerId, MessageId
from peer_data import PeerData
from config import Config
from routing_table import RoutingTable
from message.find_node import FindNode
from message.message import generate_message_id, MessageType, Message


class Peer:

    def __init__(self, peer_id: PeerId, config: Config, origin: Optional[PeerData] = None):
        self.__peer_id: PeerId = peer_id
        self.__is_origin: bool = origin is None
        self.__origin: Optional[PeerData] = origin
        self.__routing_table: RoutingTable = RoutingTable(peer_id, config)
        self.__input_queue: Queue = Queue()
        self.__listener: Thread = Thread(target=self.__listener_implementation, args=[])
        self.__core: Thread = Thread(target=self.__core_implementation, args=[])
        if not self.__is_origin:
            self.__routing_table.add_peer(self.__origin)
        # This property associates a type of message with a method (used to process it).
        self.__messages_processor: Dict[MessageType, Callable] = {
            MessageType.FIND_NODE: self.process_find_node
        }

    @property
    def input_queue(self) -> Queue:
        return self.__input_queue

    @input_queue.setter
    def input_queue(self, value: Queue) -> None:
        self.__input_queue = value

    def __listener_implementation(self) -> None:
        while True:
            print("[{0:04d}] Wait for a message...".format(self.__peer_id))
            message: Message = self.__input_queue.get()
            processor: Callable = self.__messages_processor[message.message_type]
            processor(message)

    def __core_implementation(self) -> None:
        if not self.__is_origin:
            print("[{0:04d}] Bootstrap".format(self.__peer_id))
            self.bootstrap()

    @property
    def data(self) -> PeerData:
        return PeerData(identifier=self.__peer_id, queue=self.input_queue)

    def run(self) -> None:
        self.__listener.start()
        self.__core.start()

    def join(self) -> None:
        self.__listener.join()
        self.__core.join()

    def bootstrap(self) -> MessageId:
        """
        Send the initial FIND_NODE message used to bootstrap the local peer.
        :return: the request ID that uniquely identifies the message.
        """
        if self.__is_origin:
            raise Exception("The origin cannot boostrap!")
        message_id: MessageId = generate_message_id()
        self.__origin.queue.put(FindNode(self.__peer_id, message_id))
        return message_id

    def process_find_node(self, message: FindNode) -> None:
        print("[{0:04d}] Process FIND_NODE. Message ID is {0:d}".format(self.__peer_id, message.message_id))
        pass


conf: Config = Config(list_size=5, id_length=8, alpha=3)
origin = Peer(PeerId(0), conf)
peer1 = Peer(PeerId(10), conf, origin=origin.data)

print("Start the first peer and wait 1 second")
origin.run()
sleep(1)
print("Start the other peers")
peer1.run()

origin.join()
peer1.join()
print("Done")
