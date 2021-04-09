from typing import List, Optional, Callable, Any
from kad_types import MessageId, NodeId, Timestamp
from message_supervisor.message_supervisor import MessageSupervisor


class Ping(MessageSupervisor):
    """
    The PING message is a two steps process:

    1. first, a node is tested for existence.
    2. - if the node responds, then we change the value we flag it as "most recently seen node".
       - if the node does not respond, then we remove it from the routing table and we may replace it (or not).

    Thus, ween need to save a context between the two steps.

    This class implements the necessary data structure used to manage these contexts.
    """

    def __init__(self, callback: Optional[Callable]):
        """
        Create a new supervisor for PING messages.

        :param callback: callback function executed if the (pinged) node does not respond.
        Please note that this function will be executed as a thread.
        """
        super().__init__(3, callback)

    def add(self,
            message_id: MessageId,
            expiration_timestamp: Timestamp,
            replacement_node_id: NodeId) -> None:
        """
        Place a new PING message under the supervisor responsibility.
        :param message_id: the ID of the message to supervise.
        :param expiration_timestamp: the date beyond which the message expires.
        :param replacement_node_id: the ID of the node used to replace the pinged node in the event that the
        """
        super()._add(message_id, expiration_timestamp, [replacement_node_id])

    def get(self, message_id: MessageId, auto_remove: bool = True) -> Optional[NodeId]:
        """
        Return the replacement node associated with a given PING message ID.
        :param message_id: the message ID.
        :param auto_remove: flag that tells the method whether the message context must be removed from the
        supervisor responsibility or not. The value True indicates that the message context will be removed from
        the supervisor responsibility.
        :return: if the message ID is found, the method returns the replacement node associated with a given
        message ID. Otherwise, the method returns the value None.
        """
        # First element: the ID of the replacement node.
        # Second element: the function to execute if the pinged node does not respond.
        data: Optional[List[Any]] = super()._get(message_id, auto_remove)
        return None if data is None else data[0]
