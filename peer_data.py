from typing import Optional
from kad_types import PeerId


class PeerData:

    def __init__(self, identifier: PeerId, registration_date: Optional[int] = None):
        self.__identifier: PeerId = identifier
        self.__registration_date: Optional[int] = registration_date

    @property
    def identifier(self) -> PeerId:
        return self.__identifier

    @identifier.setter
    def identifier(self, value: PeerId) -> None:
        self.__identifier = value

    @property
    def registration_date(self) -> int:
        return self.__registration_date

    @registration_date.setter
    def registration_date(self, timestamp: Optional[int]) -> None:
        self.__registration_date = timestamp

    def to_str(self, id_length) -> str:
        return ('(0xb{0:0%db}, {1:d})' % id_length).format(self.__identifier, self.__registration_date)

    def __str__(self) -> str:
        return '(0x{0:b}, {1:d})'.format(self.__identifier, self.__registration_date)

