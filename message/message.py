from typing import Optional
from kad_types import MessageId, NodeId
from random import randint
from enum import Enum


def generate_message_id() -> MessageId:
    return MessageId(randint(0, pow(10, 8)))


class MessageType(Enum):
    FIND_NODE = 0
    FIND_NODE_RESPONSE = 1
    TERMINATE_NODE = 2


class Message:

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
