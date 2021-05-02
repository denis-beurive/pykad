from typing import Optional, Dict, Any
from abc import ABC, abstractmethod
from kad_types import MessageRequestId, NodeId
from enum import Enum
from threading import Lock
from queue import Queue
from queue_manager import QueueManager
from json import dumps


class MessageName(Enum):
    FIND_NODE = 0
    FIND_NODE_RESPONSE = 1
    PING_NODE = 2
    PING_NODE_RESPONSE = 3
    TERMINATE_NODE = 4
    DISCONNECT_NODE = 5
    RECONNECT_NODE = 6


class MessageType(Enum):
    REQUEST = "request"
    RESPONSE = "response"


class MessageAction(Enum):
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
    - a request ID.
    """

    __lock_request_id_reference = Lock()
    __shared_request_id_reference: int = 0
    """Global variable used to generate unique request IDs."""
    __name_enum_to_str: Dict[MessageName, str] = {
        MessageName.FIND_NODE: "FIND_NODE",
        MessageName.FIND_NODE_RESPONSE: "FIND_NODE_RESPONSE",
        MessageName.PING_NODE: "PING_NODE",
        MessageName.PING_NODE_RESPONSE: "PING_NODE_RESPONSE",
        MessageName.TERMINATE_NODE: "TERMINATE_NODE",
        MessageName.DISCONNECT_NODE: "DISCONNECT_NODE",
        MessageName.RECONNECT_NODE: "RECONNECT_NODE"
    }
    __name_enum_to_type: Dict[MessageName, MessageType] = {
        MessageName.FIND_NODE: MessageType.REQUEST,
        MessageName.FIND_NODE_RESPONSE: MessageType.RESPONSE,
        MessageName.PING_NODE: MessageType.REQUEST,
        MessageName.PING_NODE_RESPONSE: MessageType.RESPONSE,
        MessageName.TERMINATE_NODE: MessageType.REQUEST,
        MessageName.DISCONNECT_NODE: MessageType.REQUEST,
        MessageName.RECONNECT_NODE: MessageType.REQUEST
    }
    __name_str_to_type: Dict[str, MessageType] = {
        MessageName.FIND_NODE.value: MessageType.REQUEST,
        MessageName.FIND_NODE_RESPONSE.value: MessageType.RESPONSE,
        MessageName.PING_NODE.value: MessageType.REQUEST,
        MessageName.PING_NODE_RESPONSE.value: MessageType.RESPONSE,
        MessageName.TERMINATE_NODE.value: MessageType.REQUEST,
        MessageName.DISCONNECT_NODE.value: MessageType.REQUEST,
        MessageName.RECONNECT_NODE.value: MessageType.REQUEST
    }

    def __init__(self, uid: int, request_id: MessageRequestId, message_name: MessageName, recipient_id: NodeId,
                 sender_id: Optional[NodeId] = None, args: Optional[str] = None):
        """
        Create a message.
        :param uid: message unique ID.
        :param request_id: the (unique) ID of the request.
        :param message_name: the type of the message.
        :param recipient_id: the ID of the recipient node.
        :param sender_id: the ID of the node that sends the message. Please note that the value of this
        :param args: the message argument (if any). The value of this parameter may be None. The value None is used
        for administrative messages that are not sent by nodes (typical example: the message that asks the recipient
        node to terminate its execution).
        """
        self.__uid: int = uid
        self.__request_id: MessageRequestId = request_id
        self.__message_name: MessageName = message_name
        self.__recipient_id: NodeId = recipient_id
        self.__sender_id: NodeId = sender_id
        self.__args: Optional[str] = args

    @staticmethod
    def get_new_request_id() -> MessageRequestId:
        """
        Generate a new unique request ID.
        :return: a new unique request ID.
        """
        with Message.__lock_request_id_reference:
            Message.__shared_request_id_reference += 1
            return MessageRequestId(Message.__shared_request_id_reference)

    @staticmethod
    def name_to_type(name: str) -> MessageType:
        """
        Get the type (request or response) of a message identified by its name.
        :param name: the name of the message.
        :return: the type of the message (request or response).
        """
        return Message.__name_str_to_type[name]

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
    def request_id(self) -> MessageRequestId:
        return self.__request_id

    @request_id.setter
    def request_id(self, value: MessageRequestId) -> None:
        self.__request_id = value

    @property
    def message_name(self) -> MessageName:
        return self.__message_name

    @message_name.setter
    def message_name(self, value: MessageName) -> None:
        self.__message_name = value

    @property
    def uid(self) -> int:
        return self.__uid

    @uid.setter
    def uid(self, value: int) -> None:
        self.__uid = value

    @property
    def args(self) -> Optional[str]:
        return self.__args

    @args.setter
    def args(self, value: Optional[str]) -> None:
        self.__args = value

    def message_name_str(self) -> str:
        """
        Get the textual representation the the message name.
        :return: the textual representation the the message name.
        """
        return Message.__name_enum_to_str[self.__message_name]

    def message_type(self) -> MessageType:
        """
        Get the type of the message (request or response).
        :return: the type of the message (request or response).
        """
        return Message.__name_enum_to_type[self.__message_name]

    def send(self) -> None:
        queue: Queue = QueueManager.get_queue(self.__recipient_id)
        queue.put(self)

    def _to_dict(self) -> Dict[str, Any]:
        return {
            'log-type': 'message',
            'name': self.message_name_str(),
            'uid': self.uid,
            'request_id': self.request_id,
            'sender_id': None if self.__sender_id is None else "{:d}".format(self.__sender_id),
            'recipient_id': self.__recipient_id,
            'args': self.__args
        }

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """
        Generate a key/value pairs representation of the message.
        :return: a dictionary that represents the message.
        """
        pass

    def to_json(self) -> str:
        """
        Produce a JSON representation of the message.
        :return: a JSON representation of the message.
        """
        return dumps(self.to_dict())
