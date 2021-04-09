from threading import Thread, Lock
from time import sleep, time
from typing import Dict, Tuple, List, Any, Optional, Callable
from kad_types import MessageId, Timestamp


class MessageSupervisor:

    def __init__(self, clean_period: int, callback: Optional[Callable]):
        """
        Create a message supervisor.
        :param clean_period: the cleaning period. Periodically the supervisor reviews all message and deletes the
        messages which expiration elapsed. If a callback function has been set, then this function is called on
        each message just after the message has been removed from the supervisor responsibility.
        :param callback: a function to execute when a message is removed from the supervisor responsibility while
        it has not been processed (that is: while its expiration data elapsed). Please note that if the value of this
        parameter is None, it means that no callback function is defined. In this case, no action is done on the
        message while it is removed from the supervisor responsibility.
        """
        self.__clean_period: int = clean_period
        self.__callback: Optional[Callable] = callback
        self.__messages: Dict[MessageId, Tuple[Timestamp, List[Any]]] = {}
        self.__lock = Lock()
        self.__continue = True
        self.__cleaner_thread: Thread = Thread(target=self.__cleaner, args=[])
        self.__cleaner_thread.start()

    def __cleaner(self) -> None:
        while True:
            to_remove: List[MessageId] = []
            with self.__lock:

                # Please note: you cannot modify the size of a dictionary while iterating it.
                for message_id in self.__messages.keys():
                    if self.__messages[message_id][0] < time():
                        to_remove.append(message_id)

                for message_id in to_remove:
                    args: List[Any] = self.__messages[message_id][1]
                    del self.__messages[message_id]
                    if self.__callback is not None:
                        post_process = Thread(target=self.__callback, args=args)
                        post_process.start()

            sleep(self.__clean_period)
            with self.__lock:
                again = self.__continue
            if not again:
                break

    def _add(self, message_id: MessageId, expiration_timestamp: Timestamp, args: List[Any]) -> None:
        if message_id in self.__messages:
            raise Exception("Unexpected error: the message ID {0:d} is already in use! Please note that this error "
                            "should not happen.".format(message_id))
        with self.__lock:
            self.__messages[message_id] = (expiration_timestamp, args)

    def _get(self, message_id: MessageId, auto_remove: bool) -> Optional[List[Any]]:
        with self.__lock:
            data: Optional[List[Any]] = None
            if message_id in self.__messages[message_id]:
                data = self.__messages[message_id][1]
                if auto_remove:
                    del self.__messages[message_id]
            return data

    def stop(self) -> None:
        with self.__lock:
            self.__continue = False

