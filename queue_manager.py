from typing import Dict, Optional
from queue import Queue
from kad_types import NodeId
from lock import ExtRLock


class QueueManager:
    """
    This class implements the queue manager.

    Please note that we introduce this object because we want to be able to identify a Queue object by a
    scalar value that can be injected into a database.
    """

    __lock_queues = ExtRLock("QueueManager.queues")
    __shared_queues: Dict[NodeId, Queue] = {}

    @staticmethod
    def add_queue(node_id: NodeId, queue: Queue) -> None:
        with QueueManager.__lock_queues.set("queue_manager.QueueManager.add_queue"):
            QueueManager.__shared_queues[node_id] = queue

    @staticmethod
    def get_queue(node_id: NodeId) -> Optional[Queue]:
        with QueueManager.__lock_queues.set("queue_manager.QueueManager.get_queue"):
            if node_id in QueueManager.__shared_queues:
                return QueueManager.__shared_queues[node_id]
            return None

    @staticmethod
    def is_node_running(node_id: NodeId) -> bool:
        with QueueManager.__lock_queues.set("queue_manager.QueueManager.is_node_running"):
            return node_id in QueueManager.__shared_queues

    @staticmethod
    def del_queue(node_id: NodeId) -> None:
        with QueueManager.__lock_queues.set("queue_manager.QueueManager.del_queue"):
            if node_id in QueueManager.__shared_queues:
                del QueueManager.__shared_queues[node_id]
