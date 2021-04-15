from typing import Optional, Dict, Any
from abc import ABC, abstractmethod
from kad_types import MessageId, NodeId
from enum import Enum
from threading import Lock
from queue import Queue
from queue_manager import QueueManager
from json import dumps


class MessageType(Enum):
    FIND_NODE = 0
    FIND_NODE_RESPONSE = 1
    PING_NODE = 2
    PING_NODE_RESPONSE = 3
    TERMINATE_NODE = 4
    DISCONNECT_NODE = 5
    RECONNECT_NODE = 6


class MessageDirection(Enum):
    SEND = "send"
    RECEIVE = "receive"


class Message(ABC):
    """
    This class is the base class for all classes that implement messages.
    All messages contains the following properties:

    - a unique message ID.
    - the type of the message.
    - the ID of of the sender.
    - the ID of the recipient.
    """

    __lock: Lock = Lock()
    __request_id_reference: int = 0
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
                 direction: MessageDirection,
                 uid: int,
                 request_id: MessageId,
                 message_type: MessageType,
                 recipient_id: NodeId,
                 sender_id: Optional[NodeId] = None):
        """
        Create a message.
        :param direction: the direction (send or receive).
        :param uid: message unique ID.
        :param request_id: the (unique) ID of the request.
        :param message_type: the type of the message.
        :param recipient_id: the ID of the recipient node.
        :param sender_id: the ID of the node that sends the message. Please note that the value of this
        parameter may be None. The value None is used for administrative messages that are not sent by nodes
        (typical example: the message that asks the recipient node to terminate its execution).
        """
        self.__direction = direction
        self.__uid = uid
        self.__request_id = request_id
        self.__message_type = message_type
        self.__recipient_id = recipient_id
        self.__sender_id = sender_id

    @staticmethod
    def get_new_request_id() -> MessageId:
        """
        Generate a new unique request ID.
        :return: a new unique request ID.
        """
        with Message.__lock:
            Message.__request_id_reference += 1
            return MessageId(Message.__request_id_reference)

    @property
    def direction(self) -> MessageDirection:
        return self.__direction

    @direction.setter
    def direction(self, value: MessageDirection) -> None:
        self.__direction = value

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
    def request_id(self) -> MessageId:
        return self.__request_id

    @request_id.setter
    def request_id(self, value: MessageId) -> None:
        self.__request_id = value

    @property
    def message_type(self) -> MessageType:
        return self.__message_type

    @message_type.setter
    def message_type(self, value: MessageType) -> None:
        self.__message_type = value

    @property
    def uid(self) -> int:
        return self.__uid

    @uid.setter
    def uid(self, value: int) -> None:
        self.__uid = value

    def message_type_str(self):
        return Message.__types_to_str[self.__message_type]

    def send(self) -> None:
        queue: Queue = QueueManager.get_queue(self.__recipient_id)
        queue.put(self)

    def _to_dict(self) -> Dict[str, Any]:
        return {
            'log-type': 'message',
            'direction': self.__direction.value,
            'type': self.message_type_str(),
            'uid': self.uid,
            'request_id': self.request_id,
            'sender_id': None if self.__sender_id is None else "{:d}".format(self.__sender_id),
            'recipient_id': self.__recipient_id
        }

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        pass

    def to_json(self) -> str:
        return dumps(self.to_dict())
