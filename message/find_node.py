from typing import Dict, Any
from kad_types import NodeId, MessageId
from message.message import Message, MessageType


class FindNode(Message):

    def __init__(self, uid: int, sender_id: NodeId, recipient_id: NodeId, request_id: MessageId, node_to_find_id: NodeId):
        """
        Create a new FIND_NODE message
        :param uid: message unique ID.
        :param sender_id: the ID of the node that sends the message.
        :param recipient_id: the ID of the recipient node.
        :param request_id: the (unique) ID of the message.
        :param node_to_find_id: the ID of the node to find.
        """
        self.__node_to_find_id = node_to_find_id
        super().__init__(uid, request_id, MessageType.FIND_NODE, recipient_id, sender_id)

    @property
    def node_id(self) -> NodeId:
        """
        Get the ID of the node to find.
        :return: the ID of the node to find.
        """
        return self.__node_to_find_id

    @node_id.setter
    def node_id(self, value: NodeId) -> None:
        """
        Set the ID of the node to find.
        :param value: the ID of the node to find.
        """
        self.__node_to_find_id = value

    def to_dict(self) -> Dict[str, Any]:
        d = super()._to_dict()
        d['node_to_find'] = self.node_id
        return d
