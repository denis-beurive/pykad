from typing import Tuple, List, Optional, Dict
from random import randint
from math import floor, ceil
from time import time, sleep
from threading import RLock, Lock, Thread
from kad_config import KadConfig
from bucket import Bucket
from node_data import NodeData
from kad_types import NodeId, BucketMask, BucketIndex, MessageRequestId, Timestamp
from message.message import Message, MessageAction
from message.ping_node import PingNode
from message.ping_node_reponse import PingNodeResponse
from message_supervisor.ping import Ping as PingSupervisor
from uid import Uid
from queue_manager import QueueManager
from queue import Queue
from logger import Logger


class RoutingTable:
    """
    This class implement the routing table.

    Please note:

                 The following situations may arise:

                 (1) let say that 3 new node IDs that would be stored in the same k-bucket are discovered
                     "simultaneously" (or within a short period of time).

                     If we don't take precautions, then we would send 3 PING requests to the same "least
                     recently node". Not only, there is no point in sending "simultaneously" multiple PING
                     requests to the same node, but this action would needlessly increase the network traffic
                     between nodes.

                (2) remember that node discovery results from any incoming requests. This means that the same
                    node may be discovered multiple times "simultaneously" (or within a short period of time).

                    In this case, the new discovered node must be inserted only once into a k-bucket. Indeed,
                    once a node has been inserted in a k-bucket, then it is not new anymore...

                From (1) and (2), we implement the following algorithm:

                - one "node ID FIFO" is assigned to each k-bucket. In reality we use dictionaries (see last note),
                  however, for the sake of clarity, we stick with the FIFO for the following description.
                - when a (new) node ID is discovered, we determine the k-bucket that would be used to store it.
                - if the (new) node ID is already present in the k-bucket FIFO, then the node ID is not inserted
                  (twice) into the FIFO. Otherwise, the (new) node ID is inserted into the FIFO. This takes care
                  of (2).
                - only one node from a given k-bucket is "pinged" at a time. From another point of view (the FIFO
                  point of view): node IDs in a k-bucket FIFO are processed one after the other. This takes care
                  of (1).

                Note: the order of the node IDs within k-bucket FIFOs does not matter. Thus, instead of FIFOs, we
                used dictionaries to store node IDs (because the only thing that matters is to be able to determine
                whether a given node ID has already been scheduled for "possible" insertion into the k-bucket).
    """

    def __init__(self, identifier: NodeId, config: KadConfig):
        self.__config = config
        self.__identifier = identifier
        """This local node ID."""
        self.__shared_buckets: Tuple[Bucket, ...] = tuple(Bucket(config.k) for _ in range(config.id_length))
        """The k-buckets."""
        self.__shared_insertion_pools: Tuple[Dict[NodeId, MessageRequestId], ...] = tuple({} for _ in range(config.id_length))
        """The pools used to store node IDs waiting for being inserted into the k-buckets.
        Please note: the message request ID is the request ID of the message that triggered the node ID insertion.
        The message request ID is used for logging purpose only!"""
        self.__shared_insertion_pools_busy_flags: List[bool] = list(False for _ in range(config.id_length))
        """This property associates a "busy flag" for each insertion pool. If the value if the "busy flag"
        is True, it means that that one node ID from the pool is being processed (for potential insertion
        into the appropriate k-bucket). """
        self.__bucket_masks: Tuple[BucketMask] = self.__init_bucket_masks()
        self.__ping_supervisor = PingSupervisor(self.__thread_ping_no_response)
        """This component periodically checks the status of the PING requests: have they received responses ?"""
        self.__shared_continue = True
        self.__lock_buckets = RLock()
        self.__lock_continue = RLock()
        self.__start_threads()

    def __start_threads(self) -> None:
        Thread(self.__thread_inserter()).start()

    def __thread_ping_no_response(self, message: PingNode, replacement_node_id: NodeId) -> None:
        """
        Treat the absence of (PING) response from a node. Please note that if a node failed to respond to a PING,
        then it is evicted from the k-bucket and it is replaced by the most recently seen node (that is, the node
        we just learned about).

        Please note:
        - the method will be executed by the PING supervisor.
        - the method will be executed as a thread.

        :param message: the PING message. Please keep in mind that this message is the one that has been sent by
        the local node! This is **NOT** a received message. Thus, the node to evict is the target node!
        :param replacement_node_id: the ID of the node that must be used to replace the node that does not respond
        to the PING. Please note that this is "the node we just learned about".
        """
        print("{0:04d}> Execute the callback function for PING messages that did not receive a response: "
              "{1:s}. Replacement node: {2:d}".format(self.identifier, message.to_str(), replacement_node_id))

        # Please keep in mind that this message is the one that has been sent by the local node! This is
        # **NOT** a received message. Thus, the node to evict is the target node!

        node_to_evict: NodeId = message.recipient
        with self.__lock_buckets:
            self.__evict_node(node_to_evict)
            self.add_node(replacement_node_id)
            bucket_id = self.__find_bucket_index(node_to_evict)
            self.__shared_insertion_pools_busy_flags[bucket_id] = False

    def __thread_inserter(self) -> None:
        """
        Periodically scans the insertion queues in order to find nodes that are waiting for
        potential insertion into k-buckets.
        """
        while True:
            with self.__lock_buckets:
                bucket_id: BucketIndex
                for bucket_id in range(len(self.__shared_insertion_pools)):
                    if self.__shared_insertion_pools_busy_flags[bucket_id]:
                        # One node ID from the insertion pool associated with the current k-bucket is being
                        # processed for potential injection.
                        continue
                    pool: Dict[NodeId, MessageRequestId] = self.__shared_insertion_pools[bucket_id]
                    if len(pool):
                        # Pop an item chosen at random.
                        node_id, request_id = list(pool.items()).pop()
                        # Ping the least recently seen node from the k-bucket (and, eventually, replace it).
                        self.__shared_insertion_pools_busy_flags[bucket_id] = True
                        self.__ping_for_replacement(bucket_id, node_id, request_id)

            sleep(self.__config.inserter_scanner_period)
            with self.__lock_continue:
                if not self.__shared_continue:
                    break

    def __set_bucket_insertion_pool_as_available(self, bucket_id: BucketIndex) -> None:
        self.__shared_insertion_pools_busy_flags[bucket_id] = False

    def __init_bucket_masks(self) -> Tuple[BucketMask]:
        """
        Calculate the masks used to find the bucket associated to a given node.

        Please note that these masks depends on the local node.

        Let's say that a node identifier is encoded on 8 bits (id_length = 8).

        Then:

            1. the routing table contains 8 buckets.
            2. if the local node identifier is [x7, x6, x5, x4, x3, x2, x1, x0] (with x7: MSB and x0: LSB):
               mask for bucket 0: mask0 = (x7, x6, x5, x4, x3, x2, x1, ~x0)
                                  There is only one node ID that verifies the condition:
                                  (ID >> 0) xor mask0 is 0
                                  - x7, x6, x5, x4, x3, x2, x1, ~x0
               mask for bucket 1: mask1 = (x7, x6, x5, x4, x3, x2, ~x1)
                                  There are 2 node IDs that verify the condition:
                                  (ID >> 1) xor mask1 is 0
                                  - x7, x6, x5, x4, x3, x2, ~x1, 0
                                  - x7, x6, x5, x4, x3, x2, ~x1, 1
               mask for bucket 2: mask2 = (x7, x6, x5, x4, x3, ~x2)
                                  There are 4 node IDs that verify the condition:
                                  (ID >> 2) xor mask1 is 0
                                  - x7, x6, x5, x4, x3, ~x2, 0, 0
                                  - x7, x6, x5, x4, x3, ~x2, 0, 1
                                  - x7, x6, x5, x4, x3, ~x2, 1, 0
                                  - x7, x6, x5, x4, x3, ~x2, 1, 1
               mask for bucket 3: mask3 = (x7, x6, x5, x4, ~x3)
               mask for bucket 4: mask4 = (x7, x6, x5, ~x4)
               mask for bucket 5: mask5 = (x7, x6, ~x5)
               mask for bucket 6: mask6 = (x7, ~x6)
               mask for bucket 7: mask7 = (~x7)

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
        return tuple(BucketMask((self.__identifier >> i) ^ 1) for i in range(self.__config.id_length))

    @property
    def identifier(self) -> NodeId:
        """
        Return the ID of the node that owns this routing table (that is: the ID of the local node).

        :return: the ID of the node that owns this routing table.
        """
        # Please note: the value "self.__identifier" is set once for all during the node creation.
        # And then its value is never modified. Thus, access to this property does not need to be
        # synchronized.
        return self.__identifier

    def __find_bucket_index(self, identifier: NodeId) -> Optional[BucketIndex]:
        """
        Find the bucket where to store a given node ID.

        Please note: if L is the length of a node ID (in bits), then a bucket index value is between
        0 to L-1 (included).

        :param identifier: the node ID.
        :return: if a bucket is found, then the method returns its index value. Otherwise it returns
        the value None. Please note that the only node ID that cannot be stored into a bucket is the
        ID of the local node.
        """
        index: Optional[BucketIndex] = None
        for i in range(self.__config.id_length):
            if not ((identifier >> i) ^ self.__bucket_masks[i]):
                index = BucketIndex(i)
                break
        return index

    def add_node(self, node_id: NodeId, message: Optional[Message] = None) -> None:
        """
        Add a node to the routing table.

        Please note that:
        - the node may not be inserted (into a k-bucket).
        - if the node is inserted into a k-bucket, then it may be inserter immediately or not.

        Let "KB" be the the k-bucket that would be used to store the node.
        Let "IQ" be the insertion queue associated with "KB".

        - If the node is already in "KB", then return.
        - Otherwise, if "KB" is not full, then insert the node into "KB" and return.
        - Otherwise, if the node is already waiting for insertion in "IQ", then return.
        - Otherwise, insert it into "IQ".

        Please note: once a node is inserted into an insertion queue, it is scheduled for "possible insertion".

        :param node_id: the ID of the node to add. Please note that this node must not be the local node!
        :param message: if the request for a node insertion results from the reception of a message, then this
        parameter value should be set to the message that triggered the request. Please note that the only
        situation when an insertion request is not the result of a message reception is when the well-known
        "origin" node is inserted (this should be the first node inserted into the routing table).
        :raise Exception: if the given node is the local node.
        """
        if node_id == self.__identifier:
            raise Exception("The local node {0:d} should not be inserted into the routing "
                            "table.".format(node_id))
        # Please note: the returned value (bucket_index) is greater than or equal to zero.
        # Indeed, the only node that cannot be added to the routing table is the local peer.
        # Yet, this case has already been handled.
        with self.__lock_buckets:
            bucket_index = self.__find_bucket_index(node_id)
            added, already_in = self.__shared_buckets[bucket_index].add_node(NodeData(node_id, last_seen_date=floor(time())))

            if message is None:
                # The only time we go through this branch is when the well-known "origin" node is inserted.
                # In this case, the routing table is empty since this node is the first to be inserted.
                return

            # We may need to add the node ID to the appropriate insertion queue.
            if not added and not already_in:
                if node_id not in self.__shared_insertion_pools[bucket_index]:
                    self.__shared_insertion_pools[bucket_index][node_id] = message.request_id

    def find_closest(self, node_id: NodeId, count: int) -> List[NodeId]:
        """
        Find the closest nodes to a given node.

        NOTE: this function accesses the k-buckets. But it takes care of the synchronisation.

        :param node_id: the ID of the node.
        :param count: the maximum number of node IDs to return.
        :return: the list of node IDs that are the closest ones to the given one.
        """
        with self.__lock_buckets:
            ids: List[NodeId] = []
            for bucket in self.__shared_buckets:
                ids.extend(bucket.get_all_nodes_ids())
            return sorted(ids, key=lambda pid: node_id ^ pid)[0: count]

    def __get_least_recently_seen(self, bucket_id: int) -> Optional[NodeId]:
        # The methods of class Bucket are synchronized.
        bucket: Bucket = self.__shared_buckets[bucket_id]
        return bucket.get_least_recently_seen()

    def __set_most_recently_seen(self, node_id: NodeId, bucket_idx: Optional[int] = None) -> None:
        """
        Declare a given node as the most recently seen node.

        :param node_id: the node ID to declare.
        :param bucket_idx: the index of the bucket that contains the node.
        If this parameter is not specified, then the method will find out the bucket index.
        """
        with self.__lock_buckets:
            if bucket_idx is None:
                bucket_idx = self.__find_bucket_index(node_id)
            bucket: Bucket = self.__shared_buckets[bucket_idx]
            bucket.set_most_recently_seen(node_id)

    def __evict_node(self, node_id: NodeId, bucket_idx: Optional[int] = None) -> None:
        """
        Evict a node from a bucket.

        WARNING: precautions must be taken while calling this function!
                 You must acquire the lock that synchronizes the access to the k-buckets prior
                 to calling the method.

        :param node_id: The ID of the node to evict.
        :param bucket_idx: the index of the bucket that contains the node.
        If this parameter is not specified, then the method will find out the bucket index.
        """
        if bucket_idx is None:
            bucket_idx = self.__find_bucket_index(node_id)
        bucket: Bucket = self.__shared_buckets[bucket_idx]
        bucket.remove_node(node_id)

    def get_random_node_id_within_bucket(self, bucket_index: BucketIndex) -> NodeId:
        """
        Return a node ID that belongs to a given bucket identifier by its index.
        The returned ID is drawn at random.

        The maximum number of nodes within the bucket at index I is: 2^I

        Example: let the local node identifier be 0b00000101.

                mask0 is 0b00000100 The only node that belongs to this bucket is 0b00000100.
                mask1 is 0b.0000011 The only nodes that belong to this bucket are:
                      - 0b0000011[0]
                      - 0b0000011[1]
                mask2 is 0b..000000 The only nodes that belong to this bucket are:
                      - 0b000000[00]
                      - 0b000000[01]
                      - 0b000000[10]
                      - 0b000000[11]
                mask3 is 0b...00000 The only nodes that belong to this bucket are:
                      - 0b00000[000]
                      - 0b00000[001]
                      - 0b00000[010]
                      - 0b00000[011]
                      - 0b00000[100]
                      - 0b00000[101]
                      - 0b00000[110]
                      - 0b00000[111]
                ... and so on.

        :param bucket_index: the bucket index.
        :return: a node ID that belongs to this bucket identifier by the given index.
        """
        if bucket_index not in range(0, self.__config.id_length):
            raise Exception("Unexpected bucket index {0:d}.".format(bucket_index))
        maximum = pow(2, self.__config.id_length) - 1
        v = (self.__bucket_masks[bucket_index] << bucket_index) & maximum
        for i in range(0, bucket_index):
            v = v | (randint(0, 1) << i)
        return v

    def stop(self) -> None:
        """
        Stop the execution of all threads used to maintain the routing table.
        """
        with self.__lock_continue:
            self.__shared_continue = False
        self.__ping_supervisor.stop()

    def notify_ping_response(self, message: PingNodeResponse) -> None:
        """
        This method must be called whenever the response to a PING message is received.
        It notifies the routing table that a node responded to a PING message.

        Please note that the message processing loop is implemented outside of this object.

        :param message: the message that contains the response for the PING message.
        """
        self.__ping_supervisor.delete(message.request_id)
        bucket_id = self.__find_bucket_index(message.sender_id)
        self.__set_most_recently_seen(message.sender_id, bucket_id)
        self.__set_bucket_insertion_pool_as_available(bucket_id)

    def __ping_for_replacement(self,
                               bucket_idx: int,
                               new_node_to_insert_id: NodeId,
                               message_request_id: MessageRequestId) -> None:
        """
        Ping a node in the context when we try to insert a new node into a full bucket.
        In this context, the procedure is the following:

        We ping the least recently seen node in the bucket.
        * if the least recently seen node fails to respond to the PING message, then we evict it from
          the bucket and we insert the new node.
        * if the least recently seen node responds to the PING message, then we discard the new node
          and the least recently seen node becomes the most recently seen node.

        :param bucket_idx: the index of the bucket we want to insert the new node into.
        :param new_node_to_insert_id: the ID of the new node (to insert into the bucket).
        :param message_request_id: the request ID of the message that triggered this action. Please note that
        this value is only used for logging purposes.
        """
        uid = Uid.uid()
        least_recently_seen_node_id: NodeId = self.__get_least_recently_seen(bucket_idx)
        # Ping the least recently node.
        message = PingNode(uid=uid,
                           sender_id=self.__identifier,
                           recipient_id=least_recently_seen_node_id,
                           request_id=Message.get_new_request_id())
        target_queue: Queue = QueueManager.get_queue(least_recently_seen_node_id)
        if target_queue is None:
            # This means that the target node does not exist anymore.
            print("{0:04d}> [{1:08d}] The queue for node {2:d} does not exist.".format(self.__identifier,
                                                                                       message_request_id,
                                                                                       least_recently_seen_node_id))
            self.__thread_ping_no_response(message, new_node_to_insert_id)
            return
        print("{0:04d}> [{1:08d}] {2:s}".format(self.__identifier, message_request_id, message.to_str()))
        Logger.log_message(message, MessageAction.SEND, "ping_for_replacement")
        message.send()
        # Please don't forget to add the timeout duration to the timestamp
        # (expiration_data = nox + timeout_duration)
        self.__ping_supervisor.add(message,
                                   Timestamp(ceil(time()) + self.__config.message_ping_node_timeout),
                                   new_node_to_insert_id)

    def __repr__(self) -> str:
        """
        Return a textual representation of the routing table.
        :return: a textual representation of the routing table.
        """
        with self.__lock_buckets:
            representation: List[str] = [('RT for {0:0%db}' % self.__config.id_length).format(self.__identifier),
                                         '  Bucket masks:']
            for i in range(self.__config.id_length):
                representation.append(("    {0:3d}: {1:0%db}{2:s} (test if ((id >> {3:03d}) ^ mask) == 0)" %
                                       (self.__config.id_length - i)).format(i, self.__bucket_masks[i], '.' * i, i))
            representation.append("  Bucket contents:")
            for i in range(self.__config.id_length):
                bucket: Bucket = self.__shared_buckets[i]
                representation.append("    {0:3d}: {1:3d} node(s)".format(i, bucket.count()))
                if bucket.count():
                    for p in bucket.get_all_nodes_data():
                        representation.append('             {0:s}'.format(p.to_str(self.__config.id_length)))
            return "\n".join(representation)

    def dump(self) -> str:
        with self.__lock_buckets:
            counts: List[str] = []
            for i in range(self.__config.id_length):
                bucket: Bucket = self.__shared_buckets[i]
                if bucket.count():
                    counts.append("{0:d}:[{1:s}]".format(i, ",".join([str(n) for n in bucket.get_all_nodes_ids()])))
            return "{" + " ".join(counts) + "}"
