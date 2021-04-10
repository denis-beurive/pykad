from typing import Optional, Dict, Callable
from threading import Thread
from queue import Queue
from time import time
from math import ceil
from kad_types import NodeId, MessageId, Timestamp
from node_data import NodeData
from kad_config import KadConfig
from routing_table import RoutingTable
from message.find_node import FindNode
from message.find_node_response import FindNodeResponse
from message.terminate_node import TerminateNode
from message.ping_node import PingNode
from message.ping_node_reponse import PingNodeResponse
from message.message import MessageType, Message
from queue_manager import QueueManager
from message_supervisor.ping import Ping as PingSupervisor


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
        self.__ping_supervisor = PingSupervisor(self.__ping_no_response)
        if not self.__is_origin:
            self.__routing_table.add_node(self.__origin)
        # This property associates a type of message with a method (used to process it).
        self.__messages_processor: Dict[MessageType, Callable] = {
            MessageType.TERMINATE_NODE: self.process_terminate_node,
            MessageType.FIND_NODE: self.process_find_node,
            MessageType.FIND_NODE_RESPONSE: self.process_find_node_response,
            MessageType.PING_NODE: self.process_ping,
            MessageType.PING_NODE_RESPONSE: self.process_ping_response
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
    # Callbacks executed if a message does not get any response                                                        #
    ####################################################################################################################

    def __ping_no_response(self, message: PingNode, replacement_node_id: NodeId) -> None:
        """
        Treat the absence of response from a node. Please note that if a node failed to respond to a PING, then it
        is evicted from the k-bucket and it is replaced by the most recently seen node.
        :param message: the PING message. Please keep in mind that this message is the one that has been sent by
        the local node! This is **NOT** a received message. Thus, the node to evict is the target node!
        :param replacement_node_id: the ID of the node that must be used to replace the node that does not respond to the PING.
        """
        print("{0:04d}> Execute the callback function for PING messages (no response). "
              "{1:s}. Replacement node: {2:d}".format(self.__node_id, message.to_str(), replacement_node_id))
        # Please keep in mind that this message is the one that has been sent by the local node! This is
        # **NOT** a received message. Thus, the node to evict is the target node!

        node_to_evict: NodeId = message.node_id
        self.__routing_table.evict_node(node_to_evict)
        self.__routing_table.add_node(replacement_node_id)

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
        self.__input_queue.put(TerminateNode(Message.get_new_id()))

    ####################################################################################################################

    def bootstrap(self) -> MessageId:
        """
        Send the initial FIND_NODE message used to bootstrap the local node.
        :return: the request ID that uniquely identifies the message.
        """
        if self.__is_origin:
            raise Exception("Unexpected error: the origin does not boostrap! You have an error in your code.")
        message_id = Message.get_new_id()
        recipient_queue = self.__queue_manager.get_queue(self.__origin)
        recipient_queue.put(FindNode(self.__node_id, message_id, self.__node_id))
        return message_id

    ####################################################################################################################
    # Message processor                                                                                                #
    ####################################################################################################################

    def process_terminate_node(self, message: TerminateNode) -> bool:
        print("{0:04d}> [{1:08d}] Terminate.".format(self.__node_id, message.message_id))
        self.__queue_manager.del_queue(self.__node_id)
        self.__ping_supervisor.stop()
        print("{0:04d}> [{1:08d}] Terminate DONE!".format(self.__node_id, message.message_id))
        return False

    def process_find_node(self, message: FindNode) -> bool:
        """
        Process a message of type FIND_NODE[target_node_id].

        This message is emitted by a (sender) node that needs to locate a specific node identified by its ID
        "target_node_id". The message recipient responds by sending the "k" closest nodes to "target_node_id"
        he knows about.

        The message recipient adds the sender node to its routing table.

        :param message: the message to process.
        :return: always True (which means "do not stop the node").
        """
        sender_id = message.sender_id
        message_id = message.message_id
        sender_queue = self.__queue_manager.get_queue(sender_id)
        print("{0:04d}> [{1:08d}] Process FIND_NODE from {2:d}.".format(self.__node_id, message_id, sender_id))

        # Forge a response with the same message ID and send it.
        closest = self.__routing_table.find_closest(message.node_id, self.__config.id_length)
        response = FindNodeResponse(self.__node_id, message_id, closest)
        sender_queue.put(response)

        # Add the sender ID to the routing table.
        added, already_in, bucket_idx = self.__routing_table.add_node(sender_id)
        print("{0:04d}> [{1:08d}] Node added: <{2:s}>.".format(self.__node_id,
                                                               message_id,
                                                               "yes" if added else "no"))
        if not added and not already_in:
            # The node was not added because the bucket if full.
            # We ping the least recently node.
            least_recently_seen_node_id: NodeId = self.__routing_table.get_least_recently_seen(bucket_idx)
            message = PingNode(self.__node_id, Message.get_new_id(), least_recently_seen_node_id)
            target_queue: Queue = self.__queue_manager.get_queue(least_recently_seen_node_id)
            if target_queue is None:
                print("{0:04d}> [{1:08d}] The queue for node {2:d} does not exist.".format(self.__node_id,
                                                                                           message_id,
                                                                                           least_recently_seen_node_id))
                self.__ping_no_response(message)
                return True
            print("{0:04d}> [{1:08d}] {2:s}".format(self.__node_id, message_id, message.to_str()))
            target_queue.put(message)
            self.__ping_supervisor.add(message, Timestamp(ceil(time())), sender_id)
        return True

    def process_find_node_response(self, message: FindNodeResponse) -> bool:
        print("{0:04d}> [{1:08d}] Process FIND_NODE_RESPONSE from {2:d}.".format(self.__node_id,
                                                                                 message.message_id,
                                                                                 message.sender_id))
        nodes_ids = message.node_ids
        print("{0:04d}> [{1:08d}] Nodes count: {2:d}".format(self.__node_id, message.message_id, len(nodes_ids)))
        return True

    def process_ping(self, message: PingNode) -> bool:
        target_queue: Queue = self.__queue_manager.get_queue(message.sender_id)
        if target_queue is None:
            # The node disappeared. This should not happen in this simulation.
            return True
        target_queue.put(PingNodeResponse(self.__node_id, message.message_id))
        return True

    def process_ping_response(self, message: PingNodeResponse) -> bool:
        print("{0:04d}> [{1:08d}] Process PING_NODE_RESPONSE from {2:d}.".format(self.__node_id,
                                                                                 message.message_id,
                                                                                 message.sender_id))
        self.__routing_table.set_least_recently_seen(message.sender_id)
        return True
