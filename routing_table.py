from typing import Tuple, Optional, List
from config import Config
from bucket import Bucket
from peer import Peer
from kad_types import PeerId, BucketMask, BucketIndex


class RoutingTable:

    def __init__(self, identifier: PeerId, config: Config):
        self.__list_size = config.list_size
        self.__id_length = config.id_length
        self.__alpha = config.alpha
        self.__identifier = identifier
        self.__lists: Tuple[Bucket] = tuple(Bucket(config.list_size) for _ in range(config.id_length))
        self.__side_lists: Tuple[Bucket] = tuple(Bucket(config.list_size) for _ in range(config.id_length))
        self.__bucket_masks: Tuple[BucketMask] = self.__init_bucket_masks()

    def __init_bucket_masks(self) -> Tuple[BucketMask]:
        """
        Calculate the masks used to find the bucket associated to a given peer.

        Please note that these masks depends on the local peer.

        Let's say that a peer identifier is encoded on 8 bits.

        Then:

            1. the routing table contains 8 buckets.
            2. if the local peer identifier is [x7, x6, x5, x4, x3, x2, x1, x0] (with x7: MSB and x0: LSB):
               mask for bucket 0: (x7, x6, x5, x4, x3, x2, x1, ~x0)
               mask for bucket 1: (x7, x6, x5, x4, x3, x2, ~x1)
               mask for bucket 2: (x7, x6, x5, x4, x3, ~x2)
               mask for bucket 3: (x7, x6, x5, x4, ~x3)
               mask for bucket 4: (x7, x6, x5, ~x4)
               mask for bucket 5: (x7, x6, ~x5)
               mask for bucket 6: (x7, ~x6)
               mask for bucket 7: (~x7)

               Where "~x" is the complement of x (~0=1 and ~1=0)

        For example, let the local peer identifier be 0b00000101.

        Bucket index  | Mask
        --------------|---------
        0             | 00000100
        1             | 0000011.
        2             | 000000..
        3             | 00001...
        4             | 0001....
        5             | 001.....
        6             | 01......
        7             | 1.......

        :return: the list of bucket masks.
        """
        return tuple(BucketMask((self.__identifier >> i) ^ 1) for i in range(self.__id_length))

    @property
    def identifier(self) -> PeerId:
        return self.__identifier

    @identifier.setter
    def identifier(self, value: PeerId) -> None:
        self.__identifier = value

    def __find_bucket_index(self, identifier: PeerId) -> int:
        """
        Find the bucket where to store a given peer identified by its identifier.
        :param identifier: the peer identifier.
        :return: if a bucket is found, then the method returns its index (which if a positive value).
        Otherwise the method returns the value -1. Please note that the only peer that cannot be stored into a bucket
        is the local peer.
        """
        index: int = -1
        for i in range(self.__id_length):
            if not (identifier >> i) ^ self.__bucket_masks[i]:
                index = i
                break
        return index

    def add_peer(self, peer: Peer) -> Tuple[bool, bool, BucketIndex]:
        """
        Add a peer to the routing table.
        :param peer: the peer to add. Please note that this peer must not be the local peer!
        :return: the method returns 3 values:
        - the first value tells whether the peer has been added to the routing table or not. The value True means that
          the peer has been added to the routing table. The value False means that the peer has not been added to the
          routing table.
        - in case the peer has not been added to the routing table (the first value is True), the second value tells
          whether the peer was already present in the routing table or not. The value True means that the peer was
          already present in the routing table.
        - the third value represents the index of the bucket in which the peer has been added to, or would have been
          added to (if room was available in the bucket, or if the peer was not already present in the routing table).
        :raise Exception: if the given peer is the local peer.
        """
        if peer.identifier == self.__identifier:
            raise Exception("The local peer {0:s} should not be inserted into the routing "
                            "table.".format(peer.to_str(self.__id_length)))
        # Please note: the returned value (bucket_index) is greater than or equal to zero.
        # Indeed, the only peer that cannot be added to the routing table is the local peer.
        # Yet, this case has already been handled.
        bucket_index = self.__find_bucket_index(peer.identifier)
        added, already_in = self.__lists[bucket_index].add_peer(peer)
        return added, already_in, BucketIndex(bucket_index)

    def lookup(self, destination_peer_id: PeerId) -> Optional[Peer]:
        """
        Lookup the Kademlia P2P network for a given peer identified by its given ID.
        :param destination_peer_id: the ID of the peer to lookup.
        :return: If the request peer is found, then the method returns it. Otherwise, it returns the value None.
        """
        # Get the bucket that contains the peers in the same sub-tree that the peer to look for.
        bucket_index = self.__find_bucket_index(destination_peer_id)
        closest: List[Peer] = self.__lists[bucket_index].get_closest_peers(destination_peer_id, self.__alpha)
        return None

    def __repr__(self) -> str:
        """
        Return a textual representation of the routing table.
        :return: a textual representation of the routing table.
        """
        representation: List[str] = [('RT for {0:0%db}' % self.__id_length).format(self.__identifier),
                                     '  Bucket masks:']
        for i in range(self.__id_length):
            representation.append(("    {0:3d}: {1:0%db}{2:s} (test if ((id >> {3:03d}) ^ mask) == 0)" %
                                   (self.__id_length - i)).format(i, self.__bucket_masks[i], '.' * i, i))
        representation.append("  Bucket contents:")
        for i in range(self.__id_length):
            bucket: Bucket = self.__lists[i]
            representation.append("    {0:3d}: {1:3d} peer(s)".format(i, bucket.count()))
            if bucket.count():
                for p in bucket.get_all_peers():
                    representation.append('             {0:s}'.format(p.to_str(self.__id_length)))
        return "\n".join(representation)

