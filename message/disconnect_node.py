from typing import Dict, Any
from kad_types import MessageId, NodeId
from message.message import Message, MessageType, MessageDirection


class DisconnectNode(Message):

    def __init__(self, direction: MessageDirection, uid: int, recipient_id: NodeId, request_id: MessageId):
        super().__init__(direction, uid, request_id, MessageType.DISCONNECT_NODE, recipient_id)

    def to_dict(self) -> Dict[str, Any]:
        return super()._to_dict()
