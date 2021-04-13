from data.data import Data, DataType
from kad_types import NodeId


class RoutingTable(Data):

    def __init__(self, message_uid: int, node_id: NodeId, buckets: str):
        super().__init__(message_uid, node_id, buckets)

    def type(self) -> DataType:
        return DataType.ROUTING_TABLE
