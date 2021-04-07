from kad_types import PeerId


class Peer:

    def __init__(self, identifier: PeerId, last_seen: int):
        self.__identifier: PeerId = identifier
        self.__inserted_timestamp: int = last_seen

    @property
    def identifier(self) -> PeerId:
        return self.__identifier

    @identifier.setter
    def identifier(self, value: PeerId) -> None:
        self.__identifier = value

    @property
    def inserted_timestamp(self) -> int:
        return self.__inserted_timestamp

    @inserted_timestamp.setter
    def inserted_timestamp(self, timestamp: int) -> None:
        self.__inserted_timestamp = timestamp

    def to_str(self, id_length) -> str:
        return ('(0xb{0:0%db}, {1:d})' % id_length).format(self.__identifier, self.__inserted_timestamp)

    def __str__(self) -> str:
        return '(0x{0:b}, {1:d})'.format(self.__identifier, self.__inserted_timestamp)

