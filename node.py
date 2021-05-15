from typing import Optional, Dict, Callable, List
from threading import Thread, Lock
from queue import Queue
from kad_types import NodeId, MessageRequestId
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
from message.message import MessageName, Message, MessageAction
from queue_manager import QueueManager
from logger import Logger
from uid import Uid
from data.routing_table import RoutingTable as RoutingTableData


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
        QueueManager.add_queue(self.__local_node_id, self.__input_queue)
        """Nodes talk to each other using thread queues (rather that IP). This component is 
        used to organize the threads queues."""
        if not self.__is_origin:
            self.__routing_table.add_node(self.__origin)
        self.__messages_processor: Dict[MessageName, Callable] = {
            MessageName.TERMINATE_NODE: self.__process_terminate_node,
            MessageName.FIND_NODE: self.__process_find_node,
            MessageName.FIND_NODE_RESPONSE: self.__process_find_node_response,
            MessageName.PING_NODE: self.__process_ping_node,
            MessageName.PING_NODE_RESPONSE: self.__process_ping_node_response,
            MessageName.DISCONNECT_NODE: self.__process_disconnect_node,
            MessageName.RECONNECT_NODE: self.__process_reconnect_node
        }
        """This property associates a type of message with a method used to process it."""

        # Locks and shared resources
        self.__lock_connected = Lock()
        self.__shared_connected: bool = True
        """Flag that determines whether the local node is connected or not.
        Disconnected nodes don't respond to messages."""

        # Create the threads.
        self.__listener_thread: Thread = Thread(target=self.__thread_listener, args=[])
        """Wait for messages from other nodes."""
        self.__cron_thread: Thread = Thread(target=self.__thread_cron, args=[])
        """Perform periodic node maintenance tasks."""


        # Bootstrap the node (it it is not the origin node).
        if not self.__is_origin:
            print("{0:04d}> Bootstrap".format(self.__local_node_id))
            self.__boostrap_message_id = self.__bootstrap()

    def __bootstrap(self) -> MessageRequestId:
        """
        Send the initial FIND_NODE message used to bootstrap the local node.
        :return: the request ID that uniquely identifies the message.
        """
        uid = Uid.uid()
        if self.__is_origin:
            raise Exception("Unexpected error: the origin does not boostrap! You have an error in your code.")
        message = FindNode(uid, self.__local_node_id, self.__origin, Message.get_new_request_id(), self.__local_node_id)
        Logger.log_message(message, MessageAction.SEND, "bootstrap")
        message.send()
        return message.request_id

    @property
    def data(self) -> NodeData:
        return NodeData(identifier=self.__local_node_id)

    def run(self) -> None:
        self.__start_thread()

    def __start_thread(self) -> None:
        self.__listener_thread.start()
        self.__cron_thread.start()

    def join(self, timeout=Optional[int]) -> None:
        self.__listener_thread.join(timeout=timeout)
        self.__cron_thread.join(timeout=timeout)

    def terminate(self):
        uid = Uid.uid()
        message = TerminateNode(uid, self.__local_node_id, Message.get_new_request_id())
        data = RoutingTableData(uid, self.__local_node_id, self.__routing_table.dump())
        Logger.log_message(message, MessageAction.SEND, "terminate")
        Logger.log_data(data.to_json(), "terminate")
        message.send()

    ####################################################################################################################
    # Threads                                                                                                          #
    ####################################################################################################################

    def __thread_listener(self) -> None:
        """
        Listen to messages from other nodes.

        This method expects messages on this input queue. When a message is available,
        the method executes the suitable message handler.
        """
        while True:
            print("{0:04d}> Wait for a message...".format(self.__local_node_id))

            # Extract a message from the input queue.
            # Set the "direction" and the "unique ID" properties, and then LOG the message.
            message: Message = self.__input_queue.get()

            # Execute the suitable message handler.
            processor: Callable = self.__messages_processor[message.message_name]
            with self.__lock_connected:
                if self.__shared_connected:
                    if not processor(message):
                        break
                else:
                    if message.message_name in (MessageName.TERMINATE_NODE, MessageName.RECONNECT_NODE):
                        if not processor(message):
                            break

    def __thread_cron(self) -> None:
        """
        Execute periodic tasks.
        """
        pass

    ####################################################################################################################
    # Message processor                                                                                                #
    ####################################################################################################################

    def __process_terminate_node(self, message: TerminateNode) -> bool:
        print("{0:04d}> [{1:08d}] TERMINATE_NODE.".format(self.__local_node_id, message.request_id))
        QueueManager.del_queue(self.__local_node_id)
        self.__routing_table.stop()
        return False

    def __process_disconnect_node(self, message: DisconnectNode) -> bool:
        print("{0:04d}> [{1:08d}] DISCONNECT_NODE.".format(self.__local_node_id, message.request_id))
        with self.__lock_connected:
            self.__shared_connected = False
        return True

    def __process_reconnect_node(self, message: ReconnectNode) -> bool:
        print("{0:04d}> [{1:08d}] RECONNECT_NODE.".format(self.__local_node_id, message.request_id))
        with self.__lock_connected:
            self.__shared_connected = True
        return True

    def __process_find_node(self, message: FindNode) -> bool:
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
        message_id = message.request_id
        print("{0:04d}> [{1:08d}] Process FIND_NODE from {2:d}.".format(self.__local_node_id, message_id, sender_id))

        # Forge a response with the same message ID and send it.
        uid = Uid.uid()
        closest: List[NodeId] = self.__routing_table.find_closest(message.node_id, self.__config.id_length)
        response = FindNodeResponse(uid, self.__local_node_id, sender_id, message_id, closest)
        response.send()

        # Add the sender ID to the routing table and dump the routing table.
        self.__routing_table.add_node(sender_id, message)
        data = RoutingTableData(message.uid, self.__local_node_id, self.__routing_table.dump())
        Logger.log_data(data.to_json(), "__process_find_node")
        return True

    def __process_find_node_response(self, message: FindNodeResponse) -> bool:
        print("{0:04d}> [{1:08d}] Process FIND_NODE_RESPONSE from {2:d}.".format(self.__local_node_id,
                                                                                 message.request_id,
                                                                                 message.sender_id))
        Logger.log_message(message, MessageAction.RECEIVE, "__process_find_node_response")
        nodes_ids = message.node_ids
        print("{0:04d}> [{1:08d}] Nodes count: {2:d}".format(self.__local_node_id, message.request_id, len(nodes_ids)))

        data = RoutingTableData(message.uid, self.__local_node_id, self.__routing_table.dump())
        Logger.log_data(data.to_json(), "process_find_node_response")
        # Insert the nodes into the routing table.
        for node_id in nodes_ids:
            self.__routing_table.add_node(node_id, message)

        # If this is the response to the initial FIND_NODE (used for bootstrap), then continue
        # the bootstrap sequence.
        if message.request_id == self.__boostrap_message_id:
            # TODO: send FIND_NODE messages for nodes IDs (chosen at random) located in far away buckets.
            pass
        return True

    def __process_ping_node(self, message: PingNode) -> bool:
        """
        Process a PING message: send a response.
        :param message: the PING message.
        :return: the method always returns the value True (which means that the local node should continue to run).
        """
        if QueueManager.is_node_running(message.sender_id) is None:
            # The node terminated. This should not happen in this simulation, unless the node received a
            # TERMINATE_NODE message.
            return True
        uid = Uid.uid()
        response = PingNodeResponse(uid=uid, sender_id=self.__local_node_id, recipient_id=message.sender_id,
                                    request_id=message.request_id)
        response.send()

        # Add the sender node to the routing table and dump the routing table.
        self.__routing_table.add_node(message.sender_id, message)
        data = RoutingTableData(response.uid, self.__local_node_id, self.__routing_table.dump())
        Logger.log_data(data.to_json(), "__process_find_node")
        return True

    def __process_ping_node_response(self, message: PingNodeResponse) -> bool:
        print("{0:04d}> [{1:08d}] Process PING_NODE_RESPONSE from {2:d}.".format(self.__local_node_id,
                                                                                 message.request_id,
                                                                                 message.sender_id))

        # Please don't forget remove the message from the PING supervisor.
        Logger.log_message(message, MessageAction.RECEIVE, "__process_ping_node_response")
        self.__routing_table.notify_ping_response(message)
        data = RoutingTableData(message.uid, self.__local_node_id, self.__routing_table.dump())
        Logger.log_data(data.to_json(), "process_ping_node_response")
        return True

