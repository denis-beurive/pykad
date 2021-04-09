from message.message import Message, MessageType
from kad_types import NodeId, MessageId


class PingNodeResponse(Message):

    def __init__(self, sender_id: NodeId, message_id: MessageId):
        super().__init__(message_id, MessageType.PING_NODE_RESPONSE, sender_id)
