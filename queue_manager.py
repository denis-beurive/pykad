from typing import Dict, Optional
from queue import Queue
from kad_types import NodeId
from threading import Lock


class QueueManager:
    """
    This class implements the queue manager.

    Please note that we introduce this object because we want to be able to identify a Queue object by a
    scalar value that can be injected into a database. In the long term, the dictionary used to associate
    a node ID to a Queue object will be replaced by a database table.
    """

    def __init__(self):
        self.__lock = Lock()
        self.__queues: Dict[NodeId, Queue] = {}

    def add_queue(self, node_id: NodeId, queue: Queue) -> None:
        self.__lock.acquire()
        self.__queues[node_id] = queue
        self.__lock.release()

    def get_queue(self, node_id: NodeId) -> Optional[Queue]:
        self.__lock.acquire()
        node: Optional[Queue] = None
        if node_id in self.__queues:
            node = self.__queues[node_id]
        self.__lock.release()
        return node

    def del_queue(self, node_id: NodeId) -> None:
        self.__lock.acquire()
        if node_id in self.__queues:
            del self.__queues[node_id]
        self.__lock.release()
