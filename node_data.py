from typing import Optional
from kad_types import NodeId


class NodeData:

    def __init__(self, identifier: NodeId, last_seen_date: Optional[int] = None):
        self.__identifier: NodeId = identifier
        self.__last_seen_date: Optional[int] = last_seen_date

    @property
    def identifier(self) -> NodeId:
        return self.__identifier

    @identifier.setter
    def identifier(self, value: NodeId) -> None:
        self.__identifier = value

    @property
    def last_seen_date(self) -> int:
        return self.__last_seen_date

    @last_seen_date.setter
    def last_seen_date(self, timestamp: Optional[int]) -> None:
        self.__last_seen_date = timestamp

    def to_str(self, id_length) -> str:
        return ('(0xb{0:0%db}, {1:d})' % id_length).format(self.__identifier, self.__last_seen_date)

    def __str__(self) -> str:
        return '(0x{0:b}, {1:d})'.format(self.__identifier, self.__last_seen_date)

