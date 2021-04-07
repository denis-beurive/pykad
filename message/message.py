from kad_types import MessageId
from random import randint
from enum import Enum


def generate_message_id() -> MessageId:
    return MessageId(randint(0, pow(10, 8)))


class MessageType(Enum):
    FIND_NODE = 0


class Message:

    def __init__(self, message_id: MessageId, message_type: MessageType):
        self.__message_id = message_id
        self.__message_type = message_type

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
