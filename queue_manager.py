from typing import Dict, Optional
from queue import Queue
from kad_types import PeerId


class QueueManager:
    """
    This class implements the queue manager.

    Please note that we introduce this object because we want to be able to identify a Queue object by a
    scalar value that can be injected into a database. In the long term, the dictionary used to associate
    a peer ID to a Queue object will be replaced by a database table.
    """

    def __init__(self):
        self.__queues: Dict[PeerId, Queue] = {}

    def add_queue(self, peer_id: PeerId, queue: Queue) -> None:
        self.__queues[peer_id] = queue

    def get_queue(self, peer_id: PeerId) -> Optional[Queue]:
        if peer_id in self.__queues:
            return self.__queues[peer_id]
        return None
