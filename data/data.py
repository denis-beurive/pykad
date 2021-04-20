from typing import Any, Dict
from abc import ABC, abstractmethod
from json import dumps
from enum import Enum
from kad_types import NodeId


class DataType(Enum):
    ROUTING_TABLE = "ROUTING_TABLE"


class Data(ABC):

    __data_type_tag: Dict[DataType, str] = {
        DataType.ROUTING_TABLE: "RT"
    }

    def __init__(self, message_uid: int, node_id: NodeId, data: Any):
        self.__message_uid = message_uid
        self.__data = data
        self.__node_id = node_id

    @abstractmethod
    def type(self) -> DataType:
        pass

    @property
    def data(self) -> Any:
        return self.__data

    @data.setter
    def data(self, value: Any) -> None:
        self.__data = value

    def to_json(self) -> str:
        d: Dict[str, Any] = {
            'log-type': 'data',
            'type': self.type().value,
            'message_uid': self.__message_uid,
            'node_id': self.__node_id,
            'data': "{} {}".format(Data.__data_type_tag[self.type()], self.__data)
        }
        return dumps(d)
