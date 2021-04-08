from typing import List
from message.message import Message, MessageType
from kad_types import PeerId, MessageId


class FindNodeResponse(Message):

    def __init__(self, sender_id: PeerId, message_id: MessageId, peer_ids: List[PeerId]):
        self.__peer_ids = peer_ids
        super().__init__(message_id, MessageType.FIND_NODE_RESPONSE, sender_id)

    @property
    def peer_ids(self) -> List[PeerId]:
        return self.__peer_ids

    @peer_ids.setter
    def peer_ids(self, peer_ids: List[PeerId]) -> None:
        self.__peer_ids = peer_ids
