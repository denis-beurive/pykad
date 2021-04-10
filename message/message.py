from typing import Optional
from kad_types import MessageId, NodeId
from enum import Enum
from threading import Lock


class MessageType(Enum):
    FIND_NODE = 0
    FIND_NODE_RESPONSE = 1
    PING_NODE = 2
    PING_NODE_RESPONSE = 3
    TERMINATE_NODE = 4


class Message:
    """
    This class is the base class for all classes that implement messages.
    All messages contains the following properties:
    - a unique message ID.
    - the type of the message.
    - the ID of of the sender.

    Please note that contrary to you may expect, the identity of the message recipient is not part of the message.
    In a real-life implementation, it would certainly be the case. However, this code implements a simulator that uses
    threads to implement nodes. Threads exchange data through queues. It would have been possible to specify a queue ID
    """

    __lock: Lock = Lock()
    __id: int = 0

    def __init__(self, message_id: MessageId, message_type: MessageType, sender_id: Optional[NodeId] = None):
        """
        Create a message.
        :param message_id: the (unique) ID of the message.
        :param message_type: the type of the message.
        :param sender_id: the ID of the node that sends the message. Please note that the value of this
        parameter may be None. The value None is used for administrative messages that are not sent by nodes
        (typical example: the message that asks the recipient node to terminate its execution).
        """
        self.__sender_id = sender_id
        self.__message_id = message_id
        self.__message_type = message_type

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
