from typing import Dict, Any
from kad_types import MessageId, NodeId
from message.message import Message, MessageName, MessageType


class ReconnectNode(Message):
    """
    Message used to tell a node to reconnect.
    """

    def __init__(self, uid: int, recipient_id: NodeId, request_id: MessageId):
        super().__init__(uid, request_id, MessageName.RECONNECT_NODE, recipient_id)

    def to_dict(self) -> Dict[str, Any]:
        return super()._to_dict()

