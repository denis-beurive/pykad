from kad_types import NodeId, MessageId
from message.message import Message, MessageType


class PingNode(Message):
    """
    This class implements a PING message.
    """

    def __init__(self, sender_id: NodeId, recipient_id: NodeId, message_id: MessageId):
        """
        Create a new PING message.
        :param sender_id: the ID of the node that sends the message.
        :param recipient_id: the ID of the recipient node. This is the ID of the node to ping.
        :param message_id: the message (unique) ID.
        """
        super().__init__(message_id, MessageType.PING_NODE, recipient_id, sender_id)

    def to_str(self) -> str:
        return "PING({0:08d}: {1:d} -> {2:d})".format(self.message_id, self.sender_id, self.recipient)

    def cvs(self) -> str:
        return super().csv()
