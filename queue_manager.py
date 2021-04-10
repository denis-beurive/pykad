from typing import Dict, Optional
from queue import Queue
from kad_types import NodeId
from threading import RLock


class QueueManager:
    """
    This class implements the queue manager.

    Please note that we introduce this object because we want to be able to identify a Queue object by a
    scalar value that can be injected into a database.
    """

    __lock = RLock()
    __queues: Dict[NodeId, Queue] = {}

    @staticmethod
    def add_queue(node_id: NodeId, queue: Queue) -> None:
        with QueueManager.__lock:
            QueueManager.__queues[node_id] = queue

    @staticmethod
    def get_queue(node_id: NodeId) -> Optional[Queue]:
        with QueueManager.__lock:
            if node_id in QueueManager.__queues:
                return QueueManager.__queues[node_id]
            return None

    @staticmethod
    def is_node_running(node_id: NodeId) -> bool:
        with QueueManager.__lock:
            return node_id in QueueManager.__queues

    @staticmethod
    def del_queue(node_id: NodeId) -> None:
        with QueueManager.__lock:
            if node_id in QueueManager.__queues:
                del QueueManager.__queues[node_id]
