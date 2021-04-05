from operator import attrgetter
from typing import Dict, Union, Optional
from peer import Peer


class Bucket:

    def __init__(self, size_limit: int):
        self.size_limit = size_limit
        self.identifiers: Dict[int, Peer] = dict()

    def contains(self, identifier: int) -> bool:
        return identifier in self.identifiers

    def add(self, peer: Peer) -> None:
        if len(self.identifiers) >= self.size_limit:
            raise Exception("The maximum number of elements for a Bucket is {0:%d}".format(self.size_limit))
        if self.contains(peer.identifier):
            return
        self.identifiers[peer.identifier] = peer

    def remove(self, peer: Union[int, Peer]) -> None:
        identifier = peer.identifier if isinstance(peer, Peer) else peer
        if not self.contains(peer.identifier):
            raise Exception('Unexpected identifier "{0:%d}"'.format(identifier))
        del self.identifiers[identifier]

    def last_seen(self) -> Optional[Peer]:
        if len(self.identifiers):
            return sorted(self.identifiers.values(), key=attrgetter('last_seen'))[-1]
        return None

    def __str__(self) -> str:
        return ", ".join(p.__str__() for p in self.identifiers.values())
