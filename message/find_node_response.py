from typing import Dict, Any, List
import json
from message.message import Message, MessageName, MessageType
from kad_types import NodeId, MessageRequestId


class FindNodeResponse(Message):
    """
    This class represents the response to a FIND_NODE message.
    """

    def __init__(self, uid: int, sender_id: NodeId, recipient_id: NodeId, request_id: MessageRequestId,
                 node_ids: List[NodeId]):
        self.__node_ids = node_ids
        super().__init__(uid, request_id, MessageName.FIND_NODE_RESPONSE, recipient_id, sender_id, json.dumps(node_ids))

    @property
    def node_ids(self) -> List[NodeId]:
        return self.__node_ids

    @node_ids.setter
    def node_ids(self, node_ids: List[NodeId]) -> None:
        self.__node_ids = node_ids

    def to_dict(self) -> Dict[str, Any]:
        return super()._to_dict()
