from threading import Thread, Lock
from time import sleep, time
from typing import Dict, Tuple, List, Any, Optional, Callable
from kad_types import MessageRequestId, Timestamp
from abc import ABC, abstractmethod
from message.message import Message, NodeId


class MessageSupervisor(ABC):
    """
    This class implements the message supervisor.

    Messages are sent using a connection less protocol (UDP, in practice) and the sender cannot rely on
    the underlying network infrastructure to be notified whether a message reached its destination or not.

    Some messages expect a response in return while others do not. Typically, a PING message expects a response
    while a TERMINATE message does not.

    If a message that expects a response does not receive any response, that does not necessarily mean that the
    destination disappeared. The message could have been lost on its journey from the sender node to the recipient.
    This is why, if a node does not respond to a message that expects a response, then the message is resent a
    certain number of times. This number is a configuration parameter. Let "N" be the value of this configuration
    parameter. If the sender does not receive a reply after N attempts to send a message, then the sender considers
    that the recipient has disappeared. The delay between two sending attempts is a configuration parameter.
    """

    def __init__(self, clean_period: int, callback: Optional[Callable]):
        """
        Create a message supervisor.
        :param clean_period: the cleaning period. Periodically the supervisor reviews all messages
        and deletes the messages which expiration elapsed.
        If a callback function has been set, then this function is called on
        each message just after the message has been removed from the supervisor responsibility.
        :param callback: a function to execute when a message is removed from the supervisor responsibility while
        it has not been processed (that is: while its expiration data elapsed). Please note that if the value of this
        parameter is None, it means that no callback function is defined. In this case, no action is done on the
        message while it is removed from the supervisor responsibility.
        """
        self.__clean_period: int = clean_period
        self.__callback: Optional[Callable] = callback
        self.__messages: Dict[MessageRequestId, Tuple[Timestamp, List[Any]]] = {}
        self.__lock = Lock()
        self.__continue = True
        self.__cleaner_thread: Thread = Thread(target=self.__cleaner, args=[])
        self.__cleaner_thread.start()

    def __cleaner(self) -> None:
        """
        The "cleaner thread": this thread periodically looks for unanswered PING messages.
        If it finds unanswered PING messages, then it executes the given callback (`self.__messages`).
        """
        while True:
            sleep(self.__clean_period)
            to_remove: List[MessageRequestId] = []
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

            with self.__lock:
                again = self.__continue
            if not again:
                break

    def _add(self, request_id: MessageRequestId, expiration_timestamp: Timestamp, args: List[Any]) -> None:
        """
        Add a message to the supervisor responsibility.
        :param request_id: the request ID of the message to add.
        :param expiration_timestamp: the message expiration date. After this date, it is considered
        that the message has not been answered.
        :param args: the arguments to pass to the callback function that is executed on a message if
        the expiry date for this message has passed.
        """
        with self.__lock:
            if request_id in self.__messages:
                raise Exception("Unexpected error: the message ID {0:d} is already in use! Please note that this error "
                                "should not happen.".format(request_id))
            self.__messages[request_id] = (expiration_timestamp, args)

    def _get(self, request_id: MessageRequestId, auto_remove: bool) -> Optional[List[Any]]:
        """
        Get the arguments that must be given to a callback function designed to process an unanswered message.
        :param request_id:the request ID of the message that must be given to a callback function.
        :param auto_remove: flag that tells whether the message should be suppressed from the message repository
        or not once the arguments are returned. The value True triggers the suppression of the message.
        :return: the arguments that must be given to the callback function designed to process the (unanswered) message.
        """
        with self.__lock:
            data: Optional[List[Any]] = None
            if request_id in self.__messages:
                data = self.__messages[request_id][1]
                if auto_remove:
                    del self.__messages[request_id]
            return data

    def _del(self, message_id: MessageRequestId) -> None:
        """
        Remove a message from the supervisor responsibility.
        :param message_id: the ID of the message to remove.
        """
        with self.__lock:
            if message_id in self.__messages:
                del self.__messages[message_id]

    def stop(self) -> None:
        """
        Stop the supervisor.
        """
        with self.__lock:
            self.__continue = False

    @abstractmethod
    def add(self,
            message: Message,
            expiration_timestamp: Timestamp,
            replacement_node_id: Optional[NodeId] = None) -> None:
        """
        Place a new message under the supervisor responsibility.
        :param message: the message to supervise.
        :param expiration_timestamp: the date beyond which the message expires.
        :param replacement_node_id: the ID of the node that should replace the pinged node in the routing table.
        Please note that this parameter is optional.
        """
        pass

    @abstractmethod
    def get(self, message_id: MessageRequestId, auto_remove: bool = True) -> Optional[Tuple[MessageRequestId, Optional[NodeId]]]:
        """
        Return the message associated with a given message ID.
        :param message_id: the message ID.
        :param auto_remove: flag that tells the method whether the message context must be removed from the
        supervisor responsibility or not. The value True indicates that the message context will be removed from
        the supervisor responsibility.
        :return: if the message ID is found, the method returns a tuple that contains 2 elements:
        - the message associated with a given message ID.
        - the replacement node (please note that the value of this element may be None).
        Otherwise, the method returns the value None.
        """
        pass

    @abstractmethod
    def delete(self, message_id: MessageRequestId) -> None:
        """
        Remove a message (identified by its ID) from the supervisor responsibility.
        :param message_id: the ID of the message to remove from the supervisor responsibility.
        """
        pass
