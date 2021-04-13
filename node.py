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
from uid import Uid


class Node:
    """
    This class implement a Kademlia node.
    """

    def __init__(self,
                 node_id: NodeId,
                 config: KadConfig,
                 origin: Optional[NodeId] = None):
        """
        Create a new Kademlia node.
        :param node_id: the ID of the node.
        :param config: the Kademlia configuration.
        :param origin: the ID of the "origin" node. The "origin" node is the one that is used during the
        bootstrap procedure. It is a well-known node. If the current node is the "origin" node, then the
        value of this parameter is None.
        """
        self.__config = config
        self.__local_node_id: NodeId = node_id
        self.__is_origin: bool = origin is None
        self.__origin: Optional[NodeId] = origin
        self.__routing_table: RoutingTable = RoutingTable(node_id, config)
        self.__input_queue: Queue = Queue()
        self.__boostrap_message_id: Optional[int] = None
        """The ID of the first FIND_NODE message sent in order to bootstrap the node.
        For the origin node, the value of this property is None."""
        # Declare the node's queue to the (global) "queue manager".
        QueueManager.add_queue(self.__local_node_id, self.__input_queue)
        if not self.__is_origin:
            self.__routing_table.add_node(self.__origin)
        self.__messages_processor: Dict[MessageType, Callable] = {
            MessageType.TERMINATE_NODE: self.process_terminate_node,
            MessageType.FIND_NODE: self.process_find_node,
            MessageType.FIND_NODE_RESPONSE: self.process_find_node_response,
            MessageType.PING_NODE: self.process_ping,
            MessageType.PING_NODE_RESPONSE: self.process_ping_response,
            MessageType.DISCONNECT_NODE: self.process_disconnect_node,
            MessageType.RECONNECT_NODE: self.process_reconnect_node
        }
        """This property associates a type of message with a method used to process it."""

        # Locks and shared resources
        self.__connected_lock = Lock()
        self.__connected: bool = True
        """Flag that determines whether the local node is connected or not.
        Disconnected nodes don't respond to messages."""

        # Create the threads.
        self.__listener_thread: Thread = Thread(target=self.__listener, args=[])
        self.__cron_thread: Thread = Thread(target=self.__cron, args=[])
        self.__ping_supervisor = PingSupervisor(self.__ping_no_response)
        """This component periodically checks the status of the PING requests:
        have they received responses ? The function "self.__ping_no_response" is
        executed on all requests that haven't received any response. Please mote
        that the function "self.__ping_no_response" will be executed as a thread."""

        # Bootstrap the node (it it is not the origin node).
        if not self.__is_origin:
            print("{0:04d}> Bootstrap".format(self.__local_node_id))
            self.__boostrap_message_id = self.bootstrap()

    def bootstrap(self) -> MessageId:
        """
        Send the initial FIND_NODE message used to bootstrap the local node.
        :return: the request ID that uniquely identifies the message.
        """
        uid = Uid.uid()
        if self.__is_origin:
            raise Exception("Unexpected error: the origin does not boostrap! You have an error in your code.")
        message = FindNode(self.__local_node_id, self.__origin, Message.get_new_id(), self.__local_node_id)
        self.log("M|S|{}|".format(uid) + message.csv())
        self.log("RT[{}]: {}".format(self.__local_node_id, self.__routing_table.dump()))
        message.send()
        return message.message_id

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
        uid = Uid.uid()
        message = TerminateNode(self.__local_node_id, Message.get_new_id())
        self.log("M|S|{}|".format(uid) + message.csv())
        self.log("RT[{}]: {}".format(self.__local_node_id, self.__routing_table.dump()))
        message.send()

    def log(self, message: str) -> None:
        Logger.log(message)

    ####################################################################################################################
    # Threads                                                                                                          #
    ####################################################################################################################

    def __listener(self) -> None:
        """
        Listen to messages from other nodes.

        This method expects messages on this input queue. When a message is available,
        the method execute the suitable message handler.
        """
        while True:
            uid = Uid.uid()
            print("{0:04d}> Wait for a message...".format(self.__local_node_id))
            message: Message = self.__input_queue.get()
            self.log("M|R|{}|".format(uid) + message.csv())
            processor: Callable = self.__messages_processor[message.message_type]

            with self.__connected_lock:
                if self.__connected:
                    if not processor(message, uid):
                        break
                else:
                    if message.message_type in (MessageType.TERMINATE_NODE, MessageType.RECONNECT_NODE):
                        if not processor(message, uid):
                            break

    def __cron(self) -> None:
        """
        Execute periodic tasks.
        """
        pass

    ####################################################################################################################
    # Callbacks executed if a message that should trigger a response does get any response in return.                  #
    ####################################################################################################################

    def __ping_no_response(self, message: PingNode, replacement_node_id: NodeId) -> None:
        """
        Treat the absence of response from a node. Please note that if a node failed to respond to a PING, then it
        is evicted from the k-bucket and it is replaced by the most recently seen node.

        Please note:
        - the method will be executed by the PING supervisor.
        - the method will be executed as a thread.

        :param message: the PING message. Please keep in mind that this message is the one that has been sent by
        the local node! This is **NOT** a received message. Thus, the node to evict is the target node!
        :param replacement_node_id: the ID of the node that must be used to replace the node that does not respond to the PING.
        """
        print("{0:04d}> Execute the callback function for PING messages that did not receive a response: "
              "{1:s}. Replacement node: {2:d}".format(self.__local_node_id, message.to_str(), replacement_node_id))
        # Please keep in mind that this message is the one that has been sent by the local node! This is
        # **NOT** a received message. Thus, the node to evict is the target node!

        node_to_evict: NodeId = message.recipient
        self.__routing_table.evict_node(node_to_evict)
        self.__routing_table.add_node(replacement_node_id)

    ####################################################################################################################
    # Message processor                                                                                                #
    ####################################################################################################################

    def __ping_for_replacement(self,
                               bucket_idx: int,
                               new_node_to_insert_id: NodeId,
                               message_id: MessageId) -> None:
        """
        Ping a node in the context when we try to insert a new node into a full bucket.
        In this context, the procedure is the following:
        - we take the least recently seen node in the bucket.
        - we ping thus node (the least recently seen).
          * if the least recently seen node fails to respond to the PING message, then we evict it from
            the bucket and we insert the new node.
          * if the least recently seen node responds to the PING message, then we discard the new node
            and the least recently seen node becomes the most recently seen node.
        :param bucket_idx: the index of the bucket we want to insert the new node into.
        :param new_node_to_insert_id: the ID of the new node (to insert into the bucket).
        :param message_id: the ID of the message that triggered this action. This value is only used for
        logging purposes.
        """

        uid = Uid.uid()
        least_recently_seen_node_id: NodeId = self.__routing_table.get_least_recently_seen(bucket_idx)
        message = PingNode(sender_id=self.__local_node_id,
                           recipient_id=least_recently_seen_node_id,
                           message_id=Message.get_new_id())
        target_queue: Queue = QueueManager.get_queue(least_recently_seen_node_id)
        if target_queue is None:
            print("{0:04d}> [{1:08d}] The queue for node {2:d} does not exist.".format(self.__local_node_id,
                                                                                       message_id,
                                                                                       least_recently_seen_node_id))
            self.__ping_no_response(message, new_node_to_insert_id)
            return
        print("{0:04d}> [{1:08d}] {2:s}".format(self.__local_node_id, message_id, message.to_str()))
        self.log("M|S|{}|".format(uid) + message.csv())
        # self.log("\n".join(["RT[{}]: {}".format(self.__local_node_id, line) for line in self.__routing_table.dump()]))
        message.send()
        # Please don't forget to add the timeout duration to the timestamp
        # (expiration_data = nox + timeout_duration)
        self.__ping_supervisor.add(message,
                                   Timestamp(ceil(time()) + self.__config.message_ping_node_timeout),
                                   new_node_to_insert_id)

    def process_terminate_node(self, message: TerminateNode, uid: Optional[int] = None) -> bool:
        print("{0:04d}> [{1:08d}] TERMINATE_NODE.".format(self.__local_node_id, message.message_id))
        QueueManager.del_queue(self.__local_node_id)
        self.__ping_supervisor.stop()
        return False

    def process_disconnect_node(self, message: DisconnectNode, uid: Optional[int] = None) -> bool:
        print("{0:04d}> [{1:08d}] DISCONNECT_NODE.".format(self.__local_node_id, message.message_id))
        with self.__connected_lock:
            self.__connected = False
        return True

    def process_reconnect_node(self, message: ReconnectNode, uid: Optional[int] = None) -> bool:
        print("{0:04d}> [{1:08d}] RECONNECT_NODE.".format(self.__local_node_id, message.message_id))
        with self.__connected_lock:
            self.__connected = True
        return True

    def process_find_node(self, message: FindNode, uid: Optional[int] = None) -> bool:
        """
        Process a message of type FIND_NODE[target_node_id].

        This message is emitted by a (sender) node that needs to locate a specific node identified by its ID
        "target_node_id". The message recipient responds by sending the "k" closest nodes to "target_node_id"
        he knows about.

        The message recipient adds the sender node to its routing table.

        :param message: the message to process.
        :param uid: unique ID that identifies the message.
        :return: always True (which means "do not stop the node").
        """
        sender_id = message.sender_id
        message_id = message.message_id
        print("{0:04d}> [{1:08d}] Process FIND_NODE from {2:d}.".format(self.__local_node_id, message_id, sender_id))

        # Forge a response with the same message ID and send it.
        closest = self.__routing_table.find_closest(message.node_id, self.__config.id_length)
        response = FindNodeResponse(self.__local_node_id, sender_id, message_id, closest)
        self.log("M|S|{}|".format(uid) + response.csv())
        response.send()

        # Add the sender ID to the routing table.
        added, already_in, bucket_idx = self.__routing_table.add_node(sender_id)
        print("{0:04d}> [{1:08d}] Node added: <{2:s}>.".format(self.__local_node_id,
                                                               message_id,
                                                               "yes" if added else "no"))
        self.log("RT[{}]: {}".format(self.__local_node_id, self.__routing_table.dump()))
        if not added and not already_in:
            self.__ping_for_replacement(bucket_idx, sender_id, message_id)
        return True

    def process_find_node_response(self, message: FindNodeResponse, uid: Optional[int] = None) -> bool:
        print("{0:04d}> [{1:08d}] Process FIND_NODE_RESPONSE from {2:d}.".format(self.__local_node_id,
                                                                                 message.message_id,
                                                                                 message.sender_id))
        nodes_ids = message.node_ids
        print("{0:04d}> [{1:08d}] Nodes count: {2:d}".format(self.__local_node_id, message.message_id, len(nodes_ids)))

        # Insert the nodes into the routing table.
        for node_id in nodes_ids:
            added, already_in, bucket_idx = self.__routing_table.add_node(node_id)
            if not added and not already_in:
                self.__ping_for_replacement(bucket_idx, node_id, message.message_id)
        self.log("D|RT|{}|RT[{}]: {}".format(uid, self.__local_node_id, self.__routing_table.dump()))

        # If this is the response to the initial FIND_NODE (used for bootstrap), then continue
        # the bootstrap sequence.
        if message.message_id == self.__boostrap_message_id:
            # TODO: send FIND_NODE messages for nodes IDs (chosen at random) located in far away buckets.
            pass
        return True

    def process_ping(self, message: PingNode, uid: Optional[int] = None) -> bool:
        """
        Process a PING message.
        :param message: the PING message.
        :return: the method always returns the value True (which means that the local node should continue to run).
        """
        if QueueManager.is_node_running(message.sender_id) is None:
            # The node terminated. This should not happen in this simulation, unless the node received a
            # TERMINATE_NODE message.
            return True

        message = PingNodeResponse(sender_id=self.__local_node_id,
                                   recipient_id=message.sender_id,
                                   message_id=message.message_id)
        self.log("M|S|{}|".format(uid) + message.csv())
        # TODO: add the sender to the routing table.
        # self.log("\n".join(["RT[{}]: {}".format(self.__local_node_id, line) for line in self.__routing_table.dump()]))
        message.send()
        return True

    def process_ping_response(self, message: PingNodeResponse, uid: Optional[int] = None) -> bool:
        print("{0:04d}> [{1:08d}] Process PING_NODE_RESPONSE from {2:d}.".format(self.__local_node_id,
                                                                                 message.message_id,
                                                                                 message.sender_id))
        # Please don't forget remove the message from the PING supervisor.
        self.__ping_supervisor.delete(message.message_id)
        self.__routing_table.set_least_recently_seen(message.sender_id)
        return True
