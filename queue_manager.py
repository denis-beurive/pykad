from typing import Dict, Optional
from queue import Queue
from kad_types import NodeId
from threading import RLock


class QueueManager:
    """
    This class implements the queue manager.

    Please note that we introduce this object because we want to be able to identify a Queue object by a
    scalar value that can be injected into a database. In the long term, the dictionary used to associate
    a node ID to a Queue object will be replaced by a database table.
    """

    def __init__(self):
        self.__lock = RLock()
        self.__queues: Dict[NodeId, Queue] = {}

    def add_queue(self, node_id: NodeId, queue: Queue) -> None:
        with self.__lock:
            self.__queues[node_id] = queue

    def get_queue(self, node_id: NodeId) -> Optional[Queue]:
        with self.__lock:
            if node_id in self.__queues:
                return self.__queues[node_id]
            return None

    def del_queue(self, node_id: NodeId) -> None:
        with self.__lock:
            if node_id in self.__queues:
                del self.__queues[node_id]
