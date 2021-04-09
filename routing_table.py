from typing import Tuple, List
from time import time
from math import floor
from threading import Lock
from kad_config import KadConfig
from bucket import Bucket
from node_data import NodeData
from kad_types import NodeId, BucketMask, BucketIndex


class RoutingTable:

    def __init__(self, identifier: NodeId, config: KadConfig):
        self.__bucket_size = config.bucket_size
        self.__id_length = config.id_length
        self.__alpha = config.alpha
        self.__identifier = identifier
        self.__lock = Lock()
        self.__buckets: Tuple[Bucket] = tuple(Bucket(config.bucket_size) for _ in range(config.id_length))
        self.__side_buckets: Tuple[Bucket] = tuple(Bucket(config.bucket_size) for _ in range(config.id_length))
        self.__bucket_masks: Tuple[BucketMask] = self.__init_bucket_masks()

    def __init_bucket_masks(self) -> Tuple[BucketMask]:
        """
        Calculate the masks used to find the bucket associated to a given node.

        Please note that these masks depends on the local node.

        Let's say that a node identifier is encoded on 8 bits.

        Then:

            1. the routing table contains 8 buckets.
            2. if the local node identifier is [x7, x6, x5, x4, x3, x2, x1, x0] (with x7: MSB and x0: LSB):
               mask for bucket 0: (x7, x6, x5, x4, x3, x2, x1, ~x0)
               mask for bucket 1: (x7, x6, x5, x4, x3, x2, ~x1)
               mask for bucket 2: (x7, x6, x5, x4, x3, ~x2)
               mask for bucket 3: (x7, x6, x5, x4, ~x3)
               mask for bucket 4: (x7, x6, x5, ~x4)
               mask for bucket 5: (x7, x6, ~x5)
               mask for bucket 6: (x7, ~x6)
               mask for bucket 7: (~x7)

               Where "~x" is the complement of x (~0=1 and ~1=0)

        For example, let the local node identifier be 0b00000101.

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
    def identifier(self) -> NodeId:
        return self.__identifier

    @identifier.setter
    def identifier(self, value: NodeId) -> None:
        self.__identifier = value

    def __find_bucket_index(self, identifier: NodeId) -> int:
        """
        Find the bucket where to store a given node identified by its identifier.
        :param identifier: the node identifier.
        :return: if a bucket is found, then the method returns its index (which if a positive value).
        Otherwise the method returns the value -1. Please note that the only node that cannot be stored into a bucket
        is the local node.
        """
        index: int = -1
        for i in range(self.__id_length):
            if not (identifier >> i) ^ self.__bucket_masks[i]:
                index = i
                break
        return index

    def add_node(self, node_id: NodeId) -> Tuple[bool, bool, BucketIndex]:
        """
        Add a node to the routing table.
        :param node_id: the ID of the node to add. Please note that this node must not be the local node!
        :return: the method returns 3 values:
        - the first value tells whether the node has been added to the routing table or not. The value True means that
          the node has been added to the routing table. The value False means that the peer has not been added to the
          routing table.
        - in case the node has **NOT** been added to the routing table (the first value is False), the second value
          tells whether the node was already present in the routing table or not. The value True means that the peer
          was already present in the routing table. The value False means that the node was absent from the bucket.
          This means that the bucket was full.
        - the third value represents the index of the bucket in which the node has been added to, or would have been
          added to (if room was available in the bucket, or if the node was not already present in the routing table).

        Please note that it is important to test the following return composition:
        (False, False,...): this means that the node could not be added to the routing table because the destination
        bucket was full.

        :raise Exception: if the given node is the local node.
        """
        if node_id == self.__identifier:
            raise Exception("The local node {0:d} should not be inserted into the routing "
                            "table.".format(node_id))
        # Please note: the returned value (bucket_index) is greater than or equal to zero.
        # Indeed, the only node that cannot be added to the routing table is the local peer.
        # Yet, this case has already been handled.
        self.__lock.acquire()
        bucket_index = self.__find_bucket_index(node_id)
        added, already_in = self.__buckets[bucket_index].add_node(NodeData(node_id, last_seen_date=floor(time())))
        self.__lock.release()
        return added, already_in, BucketIndex(bucket_index)

    def find_closest(self, node_id: NodeId, count: int) -> List[NodeId]:
        """
        Find the closest nodes to a given node.
        :param node_id: the ID of the node.
        :param count: the maximum number of node IDs to return.
        :return: the list of node IDs that are the closest ones to the given one.
        """
        self.__lock.acquire()
        ids: List[NodeId] = []
        for bucket in self.__buckets:
            ids.extend(bucket.get_all_nodes_ids())
        self.__lock.release()
        return sorted(ids, key=lambda pid: node_id ^ pid)[0: count]

    def __repr__(self) -> str:
        """
        Return a textual representation of the routing table.
        :return: a textual representation of the routing table.
        """
        self.__lock.acquire()
        representation: List[str] = [('RT for {0:0%db}' % self.__id_length).format(self.__identifier),
                                     '  Bucket masks:']
        for i in range(self.__id_length):
            representation.append(("    {0:3d}: {1:0%db}{2:s} (test if ((id >> {3:03d}) ^ mask) == 0)" %
                                   (self.__id_length - i)).format(i, self.__bucket_masks[i], '.' * i, i))
        representation.append("  Bucket contents:")
        for i in range(self.__id_length):
            bucket: Bucket = self.__buckets[i]
            representation.append("    {0:3d}: {1:3d} node(s)".format(i, bucket.count()))
            if bucket.count():
                for p in bucket.get_all_nodes_data():
                    representation.append('             {0:s}'.format(p.to_str(self.__id_length)))
        self.__lock.release()
        return "\n".join(representation)

