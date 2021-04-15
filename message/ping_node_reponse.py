from typing import Dict, Any
from message.message import Message, MessageType, MessageDirection
from kad_types import NodeId, MessageId


class PingNodeResponse(Message):

    def __init__(self, direction: MessageDirection, uid: int, sender_id: NodeId, recipient_id: NodeId, request_id: MessageId):
        """
        Create a new PING response message.
        :param uid: message unique ID.
        :param sender_id: the ID of the node that sends the message.
        :param recipient_id: the ID of the recipient node.
        :param request_id: the ID of the PING message.
        """
        super().__init__(direction, uid, request_id, MessageType.PING_NODE_RESPONSE, recipient_id, sender_id)

    def to_dict(self) -> Dict[str, Any]:
        return super()._to_dict()
