from typing import Optional, Dict, Callable
from threading import Thread, Lock, RLock
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
from message.disconnect_node import DisconnectNode
from message.reconnect_node import ReconnectNode
from message.ping_node import PingNode
from message.ping_node_reponse import PingNodeResponse
from message.message import MessageType, Message
from queue_manager import QueueManager
from message_supervisor.ping import Ping as PingSupervisor
from logger import Logger


class Node:

    def __init__(self,
                 node_id: NodeId,
                 config: KadConfig,
                 origin: Optional[NodeId] = None):
        self.__config = config
        self.__local_node_id: NodeId = node_id
        self.__is_origin: bool = origin is None
        self.__origin: Optional[NodeId] = origin

        # Locks and resources
        self.__connected_lock = Lock()
        self.__connected: bool = True

        self.__routing_table: RoutingTable = RoutingTable(node_id, config)
        self.__input_queue: Queue = Queue()
        QueueManager.add_queue(self.__local_node_id, self.__input_queue)
        self.__listener_thread: Thread = Thread(target=self.__listener, args=[])
        self.__cron_thread: Thread = Thread(target=self.__cron, args=[])
        self.__ping_supervisor = PingSupervisor(self.__ping_no_response)
        if not self.__is_origin:
            self.__routing_table.add_node(self.__origin)

        # This property associates a type of message with a method used to process it.
        self.__messages_processor: Dict[MessageType, Callable] = {
            MessageType.TERMINATE_NODE: self.process_terminate_node,
            MessageType.FIND_NODE: self.process_find_node,
            MessageType.FIND_NODE_RESPONSE: self.process_find_node_response,
            MessageType.PING_NODE: self.process_ping,
            MessageType.PING_NODE_RESPONSE: self.process_ping_response,
            MessageType.DISCONNECT_NODE: self.process_disconnect_node,
            MessageType.RECONNECT_NODE: self.process_reconnect_node
        }

    ####################################################################################################################
    # Threads                                                                                                          #
    ####################################################################################################################

    def __listener(self) -> None:
        while True:
            print("{0:04d}> Wait for a message...".format(self.__local_node_id))
            message: Message = self.__input_queue.get()
            self.log("M|R|" + message.csv())
            processor: Callable = self.__messages_processor[message.message_type]

            with self.__connected_lock:
                if self.__connected:
                    if not processor(message):
                        break
                else:
                    if message.message_type in (MessageType.TERMINATE_NODE, MessageType.RECONNECT_NODE):
                        if not processor(message):
                            break

    def __cron(self) -> None:
        if not self.__is_origin:
            print("{0:04d}> Bootstrap".format(self.__local_node_id))
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
              "{1:s}. Replacement node: {2:d}".format(self.__local_node_id, message.to_str(), replacement_node_id))
        # Please keep in mind that this message is the one that has been sent by the local node! This is
        # **NOT** a received message. Thus, the node to evict is the target node!

        node_to_evict: NodeId = message.recipient
        self.__routing_table.evict_node(node_to_evict)
        self.__routing_table.add_node(replacement_node_id)

    ####################################################################################################################

    @property
    def data(self) -> NodeData:
        return NodeData(identifier=self.__local_node_id)

    def run(self) -> None:
        self.__listener_thread.start()
        self.__cron_thread.start()

    def join(self, timeout=Optional[int]) -> None:
        self.__listener_thread.join(timeout=timeout)
        self.__cron_thread.join(timeout=timeout)

    def terminate(self):
        message = TerminateNode(self.__local_node_id, Message.get_new_id())
        self.log("M|S|" + message.csv())
        message.send()

    def log(self, message: str) -> None:
        Logger.log(message)

    ####################################################################################################################

    def bootstrap(self) -> MessageId:
        """
        Send the initial FIND_NODE message used to bootstrap the local node.
        :return: the request ID that uniquely identifies the message.
        """
        if self.__is_origin:
            raise Exception("Unexpected error: the origin does not boostrap! You have an error in your code.")
        message = FindNode(self.__local_node_id, self.__origin, Message.get_new_id(), self.__local_node_id)
        self.log("M|S|" + message.csv())
        message.send()
        return message.message_id

    ####################################################################################################################
    # Message processor                                                                                                #
    ####################################################################################################################

    def process_terminate_node(self, message: TerminateNode) -> bool:
        print("{0:04d}> [{1:08d}] TERMINATE_NODE.".format(self.__local_node_id, message.message_id))
        QueueManager.del_queue(self.__local_node_id)
        self.__ping_supervisor.stop()
        return False

    def process_disconnect_node(self, message: DisconnectNode) -> bool:
        print("{0:04d}> [{1:08d}] DISCONNECT_NODE.".format(self.__local_node_id, message.message_id))
        with self.__connected_lock:
            self.__connected = False
        return True

    def process_reconnect_node(self, message: ReconnectNode) -> bool:
        print("{0:04d}> [{1:08d}] RECONNECT_NODE.".format(self.__local_node_id, message.message_id))
        with self.__connected_lock:
            self.__connected = True
        return True

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
        print("{0:04d}> [{1:08d}] Process FIND_NODE from {2:d}.".format(self.__local_node_id, message_id, sender_id))

        # Forge a response with the same message ID and send it.
        closest = self.__routing_table.find_closest(message.node_id, self.__config.id_length)
        response = FindNodeResponse(self.__local_node_id, sender_id, message_id, closest)
        self.log("M|S|" + response.csv())
        response.send()

        # Add the sender ID to the routing table.
        added, already_in, bucket_idx = self.__routing_table.add_node(sender_id)
        print("{0:04d}> [{1:08d}] Node added: <{2:s}>.".format(self.__local_node_id,
                                                               message_id,
                                                               "yes" if added else "no"))
        if not added and not already_in:
            # The node was not added because the bucket if full.
            # We ping the least recently node.
            least_recently_seen_node_id: NodeId = self.__routing_table.get_least_recently_seen(bucket_idx)
            message = PingNode(self.__local_node_id,
                               least_recently_seen_node_id,
                               Message.get_new_id())
            target_queue: Queue = QueueManager.get_queue(least_recently_seen_node_id)
            if target_queue is None:
                print("{0:04d}> [{1:08d}] The queue for node {2:d} does not exist.".format(self.__local_node_id,
                                                                                           message_id,
                                                                                           least_recently_seen_node_id))
                self.__ping_no_response(message, sender_id)
                return True
            print("{0:04d}> [{1:08d}] {2:s}".format(self.__local_node_id, message_id, message.to_str()))
            self.log("M|S|" + message.csv())
            message.send()
            self.__ping_supervisor.add(message, Timestamp(ceil(time())), sender_id)
        return True

    def process_find_node_response(self, message: FindNodeResponse) -> bool:
        print("{0:04d}> [{1:08d}] Process FIND_NODE_RESPONSE from {2:d}.".format(self.__local_node_id,
                                                                                 message.message_id,
                                                                                 message.sender_id))
        nodes_ids = message.node_ids
        print("{0:04d}> [{1:08d}] Nodes count: {2:d}".format(self.__local_node_id, message.message_id, len(nodes_ids)))
        return True

    def process_ping(self, message: PingNode) -> bool:
        if QueueManager.is_node_running(message.sender_id) is None:
            # The node terminated. This should not happen in this simulation, unless the node received a
            # TERMINATE_NODE message.
            return True
        message = PingNodeResponse(self.__local_node_id, message.sender_id, message.message_id)
        self.log("M|S|" + message.csv())
        message.send()
        return True

    def process_ping_response(self, message: PingNodeResponse) -> bool:
        print("{0:04d}> [{1:08d}] Process PING_NODE_RESPONSE from {2:d}.".format(self.__local_node_id,
                                                                                 message.message_id,
                                                                                 message.sender_id))
        self.__routing_table.set_least_recently_seen(message.sender_id)
        return True
