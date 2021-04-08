from operator import attrgetter
from typing import Dict, Union, Optional, List, Tuple
from node_data import NodeData
from kad_types import NodeId


class Bucket:

    def __init__(self, size_limit: int):
        self.__size_limit = size_limit
        self.__nodes: Dict[NodeId, NodeData] = dict()

    def contains_node(self, identifier: NodeId) -> bool:
        return identifier in self.__nodes

    def count(self) -> int:
        return len(self.__nodes)

    def get_all_nodes_data(self) -> List[NodeData]:
        return list(self.__nodes.values())

    def get_all_nodes_ids(self) -> List[NodeId]:
        return [p.identifier for p in self.__nodes.values()]

    def get_closest_nodes(self, node_id: NodeId, count: int) -> List[NodeData]:
        """
        Return the closest nodes to a node identified by its given identifier.
        :param node_id: the identifier of the node used as reference.
        :param count: the maximum number of nodes that should be returned.
        :return: the function returns the list of nodes that are the closest to the one which identifier has
        been given.
        """
        if not len(self.__nodes):
            return []
        return sorted(self.__nodes.values(), key=lambda node: node.identifier ^ node_id)[0:count]

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
        if self.contains_node(node_data.identifier):
            return False, True
        if len(self.__nodes) == self.__size_limit:
            return False, False
        self.__nodes[node_data.identifier] = node_data
        return True, False

    def remove_node(self, node: Union[int, NodeData]) -> None:
        identifier = node.identifier if isinstance(node, NodeData) else node
        if not self.contains_node(node.identifier):
            raise Exception('Unexpected identifier "{0:%d}"'.format(identifier))
        del self.__nodes[identifier]

    def most_recent(self) -> Optional[NodeData]:
        if len(self.__nodes):
            return sorted(self.__nodes.values(), key=attrgetter('inserted_timestamp'))[-1]
        return None

    def oldest(self) -> Optional[NodeData]:
        if len(self.__nodes):
            return sorted(self.__nodes.values(), key=attrgetter('inserted_timestamp'))[0]
        return None

    def __str__(self) -> str:
        return ", ".join(p.__str__() for p in self.__nodes.values())
