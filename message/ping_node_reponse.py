from message.message import Message, MessageType
from kad_types import NodeId, MessageId


class PingNodeResponse(Message):

    def __init__(self, sender_id: NodeId, recipient_id: NodeId, message_id: MessageId):
        """
        Create a new PING response message.
        :param sender_id: the ID of the node that sends the message.
        :param recipient_id: the ID of the recipient node.
        :param message_id: the ID of the PING message.
        """
        super().__init__(message_id, MessageType.PING_NODE_RESPONSE, recipient_id, sender_id)

    def cvs(self) -> str:
        return super().csv()
