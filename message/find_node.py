from kad_types import PeerId, MessageId
from message.message import Message, MessageType


class FindNode(Message):

    def __init__(self, node: PeerId, message_id: MessageId):
        self.__node = node
        super().__init__(message_id, MessageType.FIND_NODE)

    @property
    def node(self) -> PeerId:
        return self.__node

    @node.setter
    def node(self, value: PeerId) -> None:
        self.__node = value

