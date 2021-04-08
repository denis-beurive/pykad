from typing import Optional, Dict, Callable
from threading import Thread
from queue import Queue
from kad_types import NodeId, MessageId
from node_data import NodeData
from kad_config import KadConfig
from routing_table import RoutingTable
from message.find_node import FindNode
from message.find_node_response import FindNodeResponse
from message.terminate_node import TerminateNode
from message.message import generate_message_id, MessageType, Message
from queue_manager import QueueManager
from ping_db import PingDb


class Node:

    def __init__(self,
                 node_id: NodeId,
                 config: KadConfig,
                 queue_manager: QueueManager,
                 origin: Optional[NodeId] = None):
        self.__config = config
        self.__node_id: NodeId = node_id
        self.__is_origin: bool = origin is None
        self.__origin: Optional[NodeId] = origin
        self.__routing_table: RoutingTable = RoutingTable(node_id, config)
        self.__queue_manager = queue_manager
        self.__input_queue: Queue = Queue()
        self.__queue_manager.add_queue(self.__node_id, self.__input_queue)
        self.__listener_thread: Thread = Thread(target=self.__listener, args=[])
        self.__cron_thread: Thread = Thread(target=self.__cron, args=[])
        self.__ping_dg: PingDb = PingDb()
        if not self.__is_origin:
            self.__routing_table.add_node(self.__origin)
        # This property associates a type of message with a method (used to process it).
        self.__messages_processor: Dict[MessageType, Callable] = {
            MessageType.TERMINATE_NODE: self.process_terminate_node,
            MessageType.FIND_NODE: self.process_find_node,
            MessageType.FIND_NODE_RESPONSE: self.process_find_node_response
        }

    ####################################################################################################################
    # Threads                                                                                                          #
    ####################################################################################################################

    def __listener(self) -> None:
        while True:
            print("{0:04d}> Wait for a message...".format(self.__node_id))
            message: Message = self.__input_queue.get()
            processor: Callable = self.__messages_processor[message.message_type]
            if not processor(message):
                break

    def __cron(self) -> None:
        if not self.__is_origin:
            print("{0:04d}> Bootstrap".format(self.__node_id))
            self.bootstrap()

    ####################################################################################################################

    @property
    def data(self) -> NodeData:
        return NodeData(identifier=self.__node_id)

    def run(self) -> None:
        self.__listener_thread.start()
        self.__cron_thread.start()

    def join(self, timeout=Optional[int]) -> None:
        self.__listener_thread.join(timeout=timeout)
        self.__cron_thread.join(timeout=timeout)

    def terminate(self):
        self.__input_queue.put(TerminateNode(generate_message_id()))

    ####################################################################################################################

    def bootstrap(self) -> MessageId:
        """
        Send the initial FIND_NODE message used to bootstrap the local node.
        :return: the request ID that uniquely identifies the message.
        """
        if self.__is_origin:
            raise Exception("Unexpected error: the origin does not boostrap! You have an error in your code.")
        message_id = generate_message_id()
        recipient_queue = self.__queue_manager.get_queue(self.__origin)
        recipient_queue.put(FindNode(self.__node_id, message_id, self.__node_id))
        return message_id

    ####################################################################################################################

    def process_terminate_node(self, message: TerminateNode) -> bool:
        print("{0:04d}> [{1:08}] Terminate.".format(self.__node_id, message.message_id))
        self.__ping_dg.stop()
        return False

    def process_find_node(self, message: FindNode) -> bool:
        """
        Process a message of type FIND_NODE.
        :param message: the message to process.
        :return: always True (which means "do not stop the node").
        """
        sender_id = message.sender_id
        message_id = message.message_id
        sender_queue = self.__queue_manager.get_queue(sender_id)
        print("{0:04d}> [{1:08}] Process FIND_NODE from {2:d}.".format(self.__node_id, message_id, sender_id))

        # Forge a response with the same message ID and send it.
        closest = self.__routing_table.find_closest(message.node_id, self.__config.id_length)
        response = FindNodeResponse(self.__node_id, message_id, closest)
        sender_queue.put(response)

        # Add the sender ID to the routing table.
        added, already_in, bucket_idx = self.__routing_table.add_node(sender_id)
        print("{0:04d}> [{1:08}] Node added: <{2:s}>.".format(self.__node_id,
                                                              message_id,
                                                              "yes" if added else "no"))
        return True

    def process_find_node_response(self, message: FindNodeResponse) -> bool:
        print("{0:04d}> [{1:08}] Process FIND_NODE_RESPONSE.".format(self.__node_id, message.message_id))
        nodes_ids = message.node_ids
        print("{0:04d}> [{1:08}] Nodes count: {2:d}".format(self.__node_id, message.message_id, len(nodes_ids)))
        return True
