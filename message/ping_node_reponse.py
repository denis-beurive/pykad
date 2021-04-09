from message.message import Message, MessageType
from kad_types import NodeId, MessageId


class PingNodeResponse(Message):

    def __init__(self, sender_id: NodeId, message_id: MessageId):
        """
        Create a new PING response message.
        :param sender_id: the ID of the node that sends the message.
        :param message_id: the ID of the PING message.
        """
        super().__init__(message_id, MessageType.PING_NODE_RESPONSE, sender_id)
