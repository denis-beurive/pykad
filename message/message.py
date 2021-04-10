from typing import Optional, Dict
from kad_types import MessageId, NodeId
from enum import Enum
from threading import Lock
from queue import Queue
from queue_manager import QueueManager


class MessageType(Enum):
    FIND_NODE = 0
    FIND_NODE_RESPONSE = 1
    PING_NODE = 2
    PING_NODE_RESPONSE = 3
    TERMINATE_NODE = 4
    DISCONNECT_NODE = 5
    RECONNECT_NODE = 6


class Message:
    """
    This class is the base class for all classes that implement messages.
    All messages contains the following properties:

    - a unique message ID.
    - the type of the message.
    - the ID of of the sender.
    - the ID of the recipient.
    """

    __lock: Lock = Lock()
    __id: int = 0
    __types_to_str: Dict[MessageType, str] = {
        MessageType.FIND_NODE: "FIND_NODE",
        MessageType.FIND_NODE_RESPONSE: "FIND_NODE_RESPONSE",
        MessageType.PING_NODE: "PING_NODE",
        MessageType.PING_NODE_RESPONSE: "PING_NODE_RESPONSE",
        MessageType.TERMINATE_NODE: "TERMINATE_NODE",
        MessageType.DISCONNECT_NODE: "DISCONNECT_NODE",
        MessageType.RECONNECT_NODE: "RECONNECT_NODE"
    }

    def __init__(self,
                 message_id: MessageId,
                 message_type: MessageType,
                 recipient_id: NodeId,
                 sender_id: Optional[NodeId] = None):
        """
        Create a message.
        :param message_id: the (unique) ID of the message.
        :param message_type: the type of the message.
        :param recipient_id: the ID of the recipient node.
        :param sender_id: the ID of the node that sends the message. Please note that the value of this
        parameter may be None. The value None is used for administrative messages that are not sent by nodes
        (typical example: the message that asks the recipient node to terminate its execution).
        """
        self.__message_id = message_id
        self.__message_type = message_type
        self.__recipient_id = recipient_id
        self.__sender_id = sender_id

    @staticmethod
    def get_new_id() -> MessageId:
        """
        Generate a new unique message ID.
        :return: a new unique message ID.
        """
        with Message.__lock:
            Message.__id += 1
            return MessageId(Message.__id)

    @property
    def sender_id(self) -> NodeId:
        return self.__sender_id

    @sender_id.setter
    def sender_id(self, value: NodeId) -> None:
        self.__sender_id = value

    @property
    def recipient(self) -> NodeId:
        return self.__recipient_id

    @recipient.setter
    def recipient(self, value: NodeId) -> None:
        self.__recipient_id = value

    @property
    def message_id(self) -> MessageId:
        return self.__message_id

    @message_id.setter
    def message_id(self, value: MessageId) -> None:
        self.__message_id = value

    @property
    def message_type(self) -> MessageType:
        return self.__message_type

    @message_type.setter
    def message_type(self, value: MessageType) -> None:
        self.__message_type = value

    def message_type_str(self):
        return Message.__types_to_str[self.__message_type]

    def send(self) -> None:
        queue: Queue = QueueManager.get_queue(self.__recipient_id)
        queue.put(self)

    def csv(self) -> str:
        return "|".join([self.message_type_str(),
                         "{:d}".format(self.__message_id),
                         "{:s}".format("None" if self.__sender_id is None else "{:d}".format(self.__sender_id)),
                         "{:d}".format(self.__recipient_id)])
