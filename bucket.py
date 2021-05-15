from operator import attrgetter
from typing import Dict, Union, Optional, List, Tuple
from node_data import NodeData
from kad_types import NodeId
from math import ceil
from time import time
from lock import ExtRLock


class Bucket:
    """
    This class implements a k-bucket.
    """

    def __init__(self, size_limit: int):
        self.__size_limit = size_limit
        self.__shared_nodes: Dict[NodeId, NodeData] = dict()
        self.__lock_nodes = ExtRLock("Bucket.nodes")

    def contains_node(self, identifier: NodeId) -> bool:
        with self.__lock_nodes.set("bucket.Bucket.contains_node"):
            return identifier in self.__shared_nodes

    def count(self) -> int:
        with self.__lock_nodes.set("bucket.Bucket.count"):
            return len(self.__shared_nodes)

    def get_all_nodes_data(self) -> List[NodeData]:
        with self.__lock_nodes.set("bucket.Bucket.get_all_nodes_data"):
            return list(self.__shared_nodes.values())

    def get_all_nodes_ids(self) -> List[NodeId]:
        with self.__lock_nodes.set("bucket.Bucket.get_all_nodes_ids"):
            return [p.identifier for p in self.__shared_nodes.values()]

    def get_closest_nodes(self, node_id: NodeId, count: int) -> List[NodeData]:
        """
        Return the closest nodes to a node identified by its given identifier.
        :param node_id: the identifier of the node used as reference.
        :param count: the maximum number of nodes that should be returned.
        :return: the function returns the list of nodes that are the closest to the one which identifier has
        been given.
        """
        with self.__lock_nodes.set("bucket.Bucket.get_closest_nodes"):
            result: List[NodeData] = []
            if len(self.__shared_nodes):
                result = sorted(self.__shared_nodes.values(), key=lambda node: node.identifier ^ node_id)[0:count]
            return result

    def add_node(self, node_data: NodeData) -> Tuple[bool, bool]:
        """
        Add a node to the bucket.
        :param node_data: the node to add.
        :return: the method returns 2 values:
        - the first value indicates whether the node was added to the bucket or not. The value True means that
          the node was added to the bucket (please note that in this case the second value is always False).
        - the second value indicates whether the node was already present in the bucket prior to the request to
          add it, or not. The value True means hat the node was already present to the bucket.
        """
        with self.__lock_nodes.set("bucket.Bucket.add_node"):
            if self.contains_node(node_data.identifier):
                return False, True

            if len(self.__shared_nodes) == self.__size_limit:
                return False, False

            self.__shared_nodes[node_data.identifier] = node_data
            return True, False

    def remove_node(self, node: Union[NodeId, NodeData]) -> None:
        """
        Evict a node from the bucket.
        :param node: the node, or node iD, to evict.
        """
        with self.__lock_nodes.set("bucket.Bucket.remove_node"):
            identifier = node.identifier if isinstance(node, NodeData) else node
            if not self.contains_node(identifier):
                raise Exception('Unexpected node identifier "{0:d}". It should be in the k-bucket.'.format(identifier))
            del self.__shared_nodes[identifier]

    def get_most_recently_seen(self) -> Optional[NodeId]:
        """
        Return the ID of the most recently seen node from the k-bucket.
        :return: If the k-bucket is not empty, then the method returns the ID of the most recently seen node it
        contains. Otherwise, it returns the value None.
        """
        with self.__lock_nodes.set("bucket.Bucket.get_most_recently_seen"):
            if len(self.__shared_nodes):
                data: NodeData = sorted(self.__shared_nodes.values(), key=attrgetter('last_seen_date'))[-1]
                return data.identifier
            return None

    def get_least_recently_seen(self) -> Optional[NodeId]:
        """
        Return the ID of the least recently seen node from the k-bucket.
        :return: If the k-bucket is not empty, then the method returns the ID of the least recently seen node it
        contains. Otherwise, it returns the value None.
        """
        with self.__lock_nodes.set("get_least_recently_seen"):
            if len(self.__shared_nodes):
                data = sorted(self.__shared_nodes.values(), key=attrgetter('last_seen_date'))[0]
                return data.identifier
            return None

    def set_most_recently_seen(self, node_id: NodeId) -> None:
        with self.__lock_nodes.set("set_most_recently_seen"):
            if node_id in self.__shared_nodes:
                self.__shared_nodes[node_id].last_seen_date = ceil(time())

    def __str__(self) -> str:
        with self.__lock_nodes.set("bucket.Bucket.__str__"):
            return ", ".join(p.__str__() for p in self.__shared_nodes.values())
