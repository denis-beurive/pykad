from typing import Dict, Any
from kad_types import NodeId, MessageId
from message.message import Message, MessageType, MessageDirection


class PingNode(Message):
    """
    This class implements a PING message.
    """

    def __init__(self, direction: MessageDirection, uid: int, sender_id: NodeId, recipient_id: NodeId, request_id: MessageId):
        """
        Create a new PING message.
        :param uid: message unique ID.
        :param sender_id: the ID of the node that sends the message.
        :param recipient_id: the ID of the recipient node. This is the ID of the node to ping.
        :param request_id: the message (unique) ID.
        """
        super().__init__(direction, uid, request_id, MessageType.PING_NODE, recipient_id, sender_id)

    def to_str(self) -> str:
        return "PING({0:08d}: {1:d} -> {2:d})".format(self.request_id, self.sender_id, self.recipient)

    def to_dict(self) -> Dict[str, Any]:
        return super()._to_dict()
