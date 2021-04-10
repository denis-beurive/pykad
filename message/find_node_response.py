from typing import List
from message.message import Message, MessageType
from kad_types import NodeId, MessageId


class FindNodeResponse(Message):

    def __init__(self, sender_id: NodeId, recipient_id: NodeId, message_id: MessageId, node_ids: List[NodeId]):
        self.__node_ids = node_ids
        super().__init__(message_id, MessageType.FIND_NODE_RESPONSE, recipient_id, sender_id)

    @property
    def node_ids(self) -> List[NodeId]:
        return self.__node_ids

    @node_ids.setter
    def node_ids(self, node_ids: List[NodeId]) -> None:
        self.__node_ids = node_ids

    def csv(self) -> str:
        return "|".join([super().csv(), "|".join(["{:d}".format(n) for n in self.__node_ids])])
