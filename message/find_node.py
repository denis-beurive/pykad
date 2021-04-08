from kad_types import NodeId, MessageId
from message.message import Message, MessageType


class FindNode(Message):

    def __init__(self, sender_id: NodeId, message_id: MessageId, node_id: NodeId):
        self.__node_id = node_id
        super().__init__(message_id, MessageType.FIND_NODE, sender_id)

    @property
    def node_id(self) -> NodeId:
        """
        Get the ID of the node to find.
        :return: the ID of the node to find.
        """
        return self.__node_id

    @node_id.setter
    def node_id(self, value: NodeId) -> None:
        """
        Set the ID of the node to find.
        :param value: the ID of the node to find.
        """
        self.__node_id = value

