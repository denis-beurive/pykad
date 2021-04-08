from time import sleep, time
from typing import Dict, Tuple, Optional
from kad_types import MessageId, NodeId, Timestamp
from threading import Lock, Thread


class PingDb:
    """
    The PING message is a two steps process.
    1. first, a node is tested for existence.
    2. - if the node responds, then we change the value we flag it as "most recently seen node".
       - if the node does not respond, then we remove it from the routing table and we may replace it (or not).

    Thus, ween need to save a context between the two steps.

    This class implements the necessary data structure used to manage these contexts.
    """

    def __init__(self):
        self.__messages: Dict[MessageId, Tuple[NodeId, Timestamp]] = {}
        self.__continue = True
        self.__lock = Lock()
        self.__cleaner_thread: Thread = Thread(target=self.__cleaner, args=[])
        self.__cleaner_thread.start()

    def add(self, message_id: MessageId, replacement_node_id: NodeId, expiration_timestamp: Timestamp) -> None:
        if message_id in self.__messages:
            raise Exception("Unexpected error: the message ID {0:d} is already in use!".format(message_id))
        self.__lock.acquire()
        self.__messages[message_id] = (replacement_node_id, expiration_timestamp)
        self.__lock.release()

    def get(self, message_id: MessageId) -> Optional[Tuple[NodeId, Timestamp]]:
        if message_id not in self.__messages:
            return None
        self.__lock.acquire()
        res = self.__messages[message_id]
        self.__lock.release()
        return res

    def stop(self) -> None:
        self.__continue = False

    def __cleaner(self) -> None:
        while self.__continue:
            self.__lock.acquire()
            for message_id in self.__messages.keys():
                if self.__messages[message_id][1] < time():
                    del self.__messages[message_id]
            self.__lock.release()
            sleep(3)
