from typing import Tuple
from config import Config
from bucket import Bucket
from peer import Peer


class RoutingTable:

    def __init__(self, identifier: int, config: Config):
        self.__identifier = identifier
        self.__lists: Tuple[Bucket[int, Peer], ...] = tuple(Bucket(config.list_size) for _ in range(config.id_length))

    @property
    def identifier(self) -> int:
        return self.__identifier

    @identifier.setter
    def identifier(self, value: int) -> None:
        self.__identifier = value

    def add_peer(self, peer: Peer) -> None:
        d: int = peer.identifier ^ self.identifier
        self.__lists[d][peer.identifier] = peer
