from kad_types import PeerId, MessageId
from message.message import Message, MessageType


class FindNode(Message):

    def __init__(self, sender_id: PeerId, message_id: MessageId, peer_id: PeerId):
        self.__peer_id = peer_id
        super().__init__(message_id, MessageType.FIND_NODE, sender_id)

    @property
    def peer_id(self) -> PeerId:
        """
        Get the ID of the peer to find.
        :return: the ID of the peer to find.
        """
        return self.__peer_id

    @peer_id.setter
    def peer_id(self, value: PeerId) -> None:
        """
        Set the ID of the peer to find.
        :param value: the ID of the peer to find.
        """
        self.__peer_id = value

