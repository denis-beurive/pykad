from typing import Optional, Callable, Tuple
from kad_types import MessageRequestId, NodeId, Timestamp
from message.ping_node import PingNode
from message_supervisor.message_supervisor import MessageSupervisor


class Ping(MessageSupervisor):
    """
    This class implements the supervisor for PING messages.

    The PING message is a two steps process:

    1. first, a node is tested for existence.
    2. - if the node responds, then we change the value we flag it as "most recently seen node".
       - if the node does not respond, then we remove it from the routing table and we may replace it (or not).

    Thus, we need to save a context between the two steps. This class implements the necessary data structure
    used to manage these contexts.

    Please note: the PING request is used, in particular, when the current node discovers a new node ID.

                 In this case, if the k-bucket that would be used to store the newly discovered node ID is full,
                 then we send a PING request to the least recently seen node of the k-bucket.

                 The following situations may arise:

                 (1) let say that 3 new node IDs that would be stored in the same k-bucket are discovered
                     "simultaneously" (or within a short period of time).

                     If we don't take precautions, then we would send 3 PING requests to the same "least
                     recently node". Not only, there is no point in sending "simultaneously" multiple PING
                     requests to the same node, but this action would needlessly increase the network traffic
                     between nodes.

                (2) remember that node discovery results from any incoming requests. This means that the same
                    node may be discovered multiple times "simultaneously" (or within a short period of time).

                    In this case, the new discovered node must be inserted only once into a k-bucket. Indeed,
                    once a node has been inserted in a k-bucket, then it is not new anymore...

                From (1) and (2), we implement the following algorithm:

                - one "node ID FIFO" is assigned to each k-bucket. In reality we use dictionaries (see last note),
                  however, for the sake of clarity, we stick with the FIFO for the following description.
                - when a (new) node ID is discovered, we determine the k-bucket that would be used to store it.
                - if the (new) node ID is already present in the k-bucket FIFO, then the node ID is not inserted
                  (twice) into the FIFO. Otherwise, the (new) node ID is inserted into the FIFO. This takes care
                  of (2).
                - only one node from a given k-bucket is "pinged" at a time. From another point of view (the FIFO
                  point of view): node IDs in a k-bucket FIFO are processed one after the other. This takes care
                  of (1).

                Note: the order of the node IDs within k-bucket FIFOs does not matter. Thus, instead of FIFOs, we
                used dictionaries to store node IDs (because the only thing that matters is to be able to determine
                whether a given node ID has already been scheduled for "possible" insertion into the k-bucket).
    """

    def __init__(self, callback: Optional[Callable]):
        """
        Create a new supervisor for PING messages.

        :param callback: callback function executed if the (pinged) node does not respond.
        Please note that this function will be executed as a thread.
        """
        super().__init__(3, callback)

    def add(self,
            message: PingNode,
            expiration_timestamp: Timestamp,
            replacement_node_id: Optional[NodeId] = None) -> None:
        """
        Place a new PING message under the supervisor responsibility.
        :param message: the message to supervise.
        :param expiration_timestamp: the date beyond which the message expires.
        :param replacement_node_id: the ID of the node that should replace the pinged node in the routing table.
        Please note that this parameter is optional.
        """
        super()._add(message.request_id, expiration_timestamp, [message, replacement_node_id])

    def get(self, message_id: MessageRequestId, auto_remove: bool = True) -> Optional[Tuple[PingNode, Optional[NodeId]]]:
        """
        Return the message associated with a given PING message ID.
        :param message_id: the message ID.
        :param auto_remove: flag that tells the method whether the message context must be removed from the
        supervisor responsibility or not. The value True indicates that the message context will be removed from
        the supervisor responsibility.
        :return: if the message ID is found, the method returns a tuple that contains 2 elements:
        - the message associated with a given message ID.
        - the replacement node (please note that the value of this element may be None).
        Otherwise, the method returns the value None.
        """
        # First element: the ID of the replacement node.
        # Second element: the function to execute if the pinged node does not respond.
        data: Optional[Tuple[PingNode, Optional[NodeId]]] = super()._get(message_id, auto_remove)
        return None if data is None else data

    def delete(self, message_id: MessageRequestId) -> None:
        """
        Remove a PING message (identified by its ID) from the supervisor responsibility.
        :param message_id: the ID of the message to remove from the supervisor responsibility.
        """
        super()._del(message_id)

