from kad_types import MessageId
from message.message import Message, MessageType


class TerminateNode(Message):

    def __init__(self, message_id: MessageId):
        super().__init__(message_id, MessageType.TERMINATE_NODE)
