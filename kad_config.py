from typing import Dict, Any
from loggable import Loggable


class KadConfig(Loggable):

    def __init__(self,
                 id_length=128,
                 alpha=3,
                 k=20,
                 message_find_node_timeout: int = 3,
                 message_ping_node_timeout: int = 3,
                 inserter_scanner_period: int = 1):
        self.__id_length: int = id_length
        self.__alpha: int = alpha
        self.__k: int = k
        self.__message_find_node_timeout: int = message_find_node_timeout
        self.__message_ping_node_timeout: int = message_ping_node_timeout
        self.__inserter_scanner_period: int = inserter_scanner_period

    @property
    def id_length(self) -> int:
        """
        The length in bits of a node ID.
        """
        return self.__id_length

    @id_length.setter
    def id_length(self, value: int) -> None:
        self.__id_length = value

    @property
    def alpha(self) -> int:
        """
        The system wide concurrency parameter called "alpha".
        This value is used while locating the k closest nodes to a given node.
        """
        return self.__alpha

    @alpha.setter
    def alpha(self, value: int) -> None:
        self.__alpha = value

    @property
    def k(self) -> int:
        """
        The system wide replication parameter. Buckets contains k node IDs.
        """
        return self.__k

    @k.setter
    def k(self, value: int) -> None:
        self.__k = value

    @property
    def message_find_node_timeout(self) -> int:
        return self.__message_find_node_timeout

    @message_find_node_timeout.setter
    def message_find_node_timeout(self, value: int) -> None:
        self.__message_find_node_timeout = value

    @property
    def message_ping_node_timeout(self) -> int:
        return self.__message_ping_node_timeout

    @message_ping_node_timeout.setter
    def message_ping_node_timeout(self, value: int) -> None:
        self.__message_ping_node_timeout = value

    @property
    def inserter_scanner_period(self) -> int:
        return self.__inserter_scanner_period

    @inserter_scanner_period.setter
    def inserter_scanner_period(self, value: int) -> None:
        self.__inserter_scanner_period = value

    def to_dict(self) -> Dict[str, Any]:
        return {
            'log-type': 'config',
            'id_length': self.id_length,
            'alpha': self.alpha,
            'k': self.k
        }
