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

    __lock_queues = RLock()
    __shared_queues: Dict[NodeId, Queue] = {}

    @staticmethod
    def add_queue(node_id: NodeId, queue: Queue) -> None:
        with QueueManager.__lock_queues:
            QueueManager.__shared_queues[node_id] = queue

    @staticmethod
    def get_queue(node_id: NodeId) -> Optional[Queue]:
        with QueueManager.__lock_queues:
            if node_id in QueueManager.__shared_queues:
                return QueueManager.__shared_queues[node_id]
            return None

    @staticmethod
    def is_node_running(node_id: NodeId) -> bool:
        with QueueManager.__lock_queues:
            return node_id in QueueManager.__shared_queues

    @staticmethod
    def del_queue(node_id: NodeId) -> None:
        with QueueManager.__lock_queues:
            if node_id in QueueManager.__shared_queues:
                del QueueManager.__shared_queues[node_id]
