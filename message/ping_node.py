from kad_types import NodeId, MessageId
from message.message import Message, MessageType


class PingNode(Message):
    """
    This class implements a PING message.
    """

    def __init__(self, sender_id: NodeId, message_id: MessageId, node_id: NodeId):
        """
        Create a new PING message.
        :param sender_id: the ID of the node that sends the message.
        :param message_id: the message (unique) ID.
        :param node_id: the ID of the node to ping.
        """
        self.__node_id = node_id
        super().__init__(message_id, MessageType.PING_NODE, sender_id)

    @property
    def node_id(self) -> NodeId:
        """
        Get the ID of the node to ping.
        :return: the ID of the node to ping.
        """
        return self.__node_id

    @node_id.setter
    def node_id(self, value: NodeId) -> None:
        """
        Set the ID of the node to ping.
        :param value: the ID of the node to ping.
        """
        self.__node_id = value
