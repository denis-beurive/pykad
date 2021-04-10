from typing import List
from kad_types import MessageId, NodeId
from message.message import Message, MessageType


class DisconnectNode(Message):

    def __init__(self, recipient_id: NodeId, message_id: MessageId):
        super().__init__(message_id, MessageType.DISCONNECT_NODE, recipient_id)

    def csv(self) -> str:
        return super().csv()
