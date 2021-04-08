from typing import Optional, Dict, Callable
from threading import Thread
from queue import Queue
from time import time
from math import floor
from kad_types import PeerId, MessageId
from peer_data import PeerData
from kad_config import KadConfig
from routing_table import RoutingTable
from message.find_node import FindNode
from message.find_node_response import FindNodeResponse
from message.terminate_node import TerminateNode
from message.message import generate_message_id, MessageType, Message
from queue_manager import QueueManager



class Peer:

    def __init__(self,
                 peer_id: PeerId,
                 config: KadConfig,
                 queue_manager: QueueManager,
                 origin: Optional[PeerData] = None):
        self.__config = config
        self.__peer_id: PeerId = peer_id
        self.__is_origin: bool = origin is None
        self.__origin: Optional[PeerData] = origin
        self.__routing_table: RoutingTable = RoutingTable(peer_id, config)
        self.__queue_manager = queue_manager
        self.__input_queue: Queue = Queue()
        self.__queue_manager.add_queue(self.__peer_id, self.__input_queue)
        self.__listener: Thread = Thread(target=self.__listener_implementation, args=[])
        self.__core: Thread = Thread(target=self.__core_implementation, args=[])
        if not self.__is_origin:
            self.__routing_table.add_peer(self.__origin)
        # This property associates a type of message with a method (used to process it).
        self.__messages_processor: Dict[MessageType, Callable] = {
            MessageType.TERMINATE_NODE: self.process_terminate_node,
            MessageType.FIND_NODE: self.process_find_node,
            MessageType.FIND_NODE_RESPONSE: self.process_find_node_response
        }

    def __listener_implementation(self) -> None:
        while True:
            print("{0:04d}> Wait for a message...".format(self.__peer_id))
            message: Message = self.__input_queue.get()
            processor: Callable = self.__messages_processor[message.message_type]
            if not processor(message):
                break

    def __core_implementation(self) -> None:
        if not self.__is_origin:
            print("{0:04d}> Bootstrap".format(self.__peer_id))
            self.bootstrap()

    @property
    def data(self) -> PeerData:
        return PeerData(identifier=self.__peer_id)

    def run(self) -> None:
        self.__listener.start()
        self.__core.start()

    def join(self, timeout=Optional[int]) -> None:
        self.__listener.join(timeout=timeout)
        self.__core.join(timeout=timeout)

    def terminate(self):
        self.__input_queue.put(TerminateNode(generate_message_id()))

    ####################################################################################################################

    def bootstrap(self) -> MessageId:
        """
        Send the initial FIND_NODE message used to bootstrap the local peer.
        :return: the request ID that uniquely identifies the message.
        """
        if self.__is_origin:
            raise Exception("Unexpected error: the origin does not boostrap! You have an error in your code.")
        message_id = generate_message_id()
        recipient_queue = self.__queue_manager.get_queue(self.__origin.identifier)
        recipient_queue.put(FindNode(self.__peer_id, message_id, self.__peer_id))
        return message_id

    ####################################################################################################################

    def process_terminate_node(self, message: TerminateNode) -> bool:
        print("{0:04d}> [{1:08}] Terminate.".format(self.__peer_id, message.message_id))
        return False

    def process_find_node(self, message: FindNode) -> bool:
        """
        Process a message of type FIND_NODE.
        :param message: the message to process.
        :return: always True (which means "do not stop the peer").
        """
        sender_id = message.sender_id
        message_id = message.message_id
        sender_queue = self.__queue_manager.get_queue(sender_id)
        print("{0:04d}> [{1:08}] Process FIND_NODE from {2:d}.".format(self.__peer_id, message_id, sender_id))

        # Forge a response with the same message ID and send it.
        closest = self.__routing_table.find_closest(message.peer_id, self.__config.id_length)
        response = FindNodeResponse(self.__peer_id, message_id, closest)
        sender_queue.put(response)

        # Add the sender ID to the routing table.
        added, already_in, bucket_idx = self.__routing_table.add_peer(PeerData(sender_id, floor(time())))
        print("{0:04d}> [{1:08}] Node added: <{2:s}>.".format(self.__peer_id,
                                                              message_id,
                                                              "yes" if added else "no"))
        return True

    def process_find_node_response(self, message: FindNodeResponse) -> bool:
        print("{0:04d}> [{1:08}] Process FIND_NODE_RESPONSE.".format(self.__peer_id, message.message_id))
        peer_ids = message.peer_ids
        print("{0:04d}> [{1:08}] Peer count: {2:d}".format(self.__peer_id, message.message_id, len(peer_ids)))
        return True
