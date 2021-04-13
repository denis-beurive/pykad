from typing import Dict, Any, List
from message.message import Message, MessageType
from kad_types import NodeId, MessageId


class FindNodeResponse(Message):

    def __init__(self, uid: int, sender_id: NodeId, recipient_id: NodeId, request_id: MessageId, node_ids: List[NodeId]):
        self.__node_ids = node_ids
        super().__init__(uid, request_id, MessageType.FIND_NODE_RESPONSE, recipient_id, sender_id)

    @property
    def node_ids(self) -> List[NodeId]:
        return self.__node_ids

    @node_ids.setter
    def node_ids(self, node_ids: List[NodeId]) -> None:
        self.__node_ids = node_ids

    def to_dict(self) -> Dict[str, Any]:
        d = super()._to_dict()
        d['nodes_ids'] = self.node_ids
        return d
