from operator import attrgetter
from typing import Dict, Union, Optional, List, Tuple
from peer import Peer
from kad_types import PeerId


class Bucket:

    def __init__(self, size_limit: int):
        self.__size_limit = size_limit
        self.__peers: Dict[PeerId, Peer] = dict()

    def contains_peer(self, identifier: PeerId) -> bool:
        return identifier in self.__peers

    def count(self) -> int:
        return len(self.__peers)

    def get_all_peers(self) -> List[Peer]:
        return list(self.__peers.values())

    def get_closest_peers(self, peer_id: PeerId, count: int) -> List[Peer]:
        """
        Return the closest peers to a peer identified by its given identifier.
        :param peer_id: the identifier of the peer used as reference.
        :param count: the maximum number of peers that should be returned.
        :return: the function returns the list of peers that are the closest to the one which identifier has
        been given.
        """
        if not len(self.__peers):
            return []
        return sorted(self.__peers.values(), key=lambda peer: peer.identifier ^ peer_id)[0:count]

    def add_peer(self, peer: Peer) -> Tuple[bool, bool]:
        """
        Add a peer to the bucket.
        :param peer: the peer to add.
        :return: the method returns 2 values:
        - the first value indicates whether the peer was added to the bucket or not. The value True means that
          the peer was added to the bucket (please note that in this case the second value is always False).
        - the second value indicates whether the peer was already present in the bucket prior to the request to
          add it, or not. The value True means hat the peer was already present to the bucket.
        """
        if self.contains_peer(peer.identifier):
            return False, True
        if len(self.__peers) == self.__size_limit:
            return False, False
        self.__peers[peer.identifier] = peer
        return True, False

    def remove_peer(self, peer: Union[int, Peer]) -> None:
        identifier = peer.identifier if isinstance(peer, Peer) else peer
        if not self.contains_peer(peer.identifier):
            raise Exception('Unexpected identifier "{0:%d}"'.format(identifier))
        del self.__peers[identifier]

    def most_recent(self) -> Optional[Peer]:
        if len(self.__peers):
            return sorted(self.__peers.values(), key=attrgetter('inserted_timestamp'))[-1]
        return None

    def oldest(self) -> Optional[Peer]:
        if len(self.__peers):
            return sorted(self.__peers.values(), key=attrgetter('inserted_timestamp'))[0]
        return None

    def __str__(self) -> str:
        return ", ".join(p.__str__() for p in self.__peers.values())
