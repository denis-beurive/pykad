
class KadConfig:

    def __init__(self,
                 list_size=20,
                 id_length=128,
                 alpha=3, k=20,
                 message_find_node_timeout: int = 3,
                 message_ping_node_timeout: int = 3):
        self.__bucket_size: int = list_size
        self.__id_length: int = id_length
        self.__alpha: int = alpha
        self.__k: int = k
        self.__message_find_node_timeout: int = message_find_node_timeout
        self.__message_ping_node_timeout: int = message_ping_node_timeout

    @property
    def bucket_size(self) -> int:
        return self.__bucket_size

    @bucket_size.setter
    def bucket_size(self, value: int) -> None:
        self.__bucket_size = value

    @property
    def id_length(self) -> int:
        return self.__id_length

    @id_length.setter
    def id_length(self, value: int) -> None:
        self.__id_length = value

    @property
    def alpha(self) -> int:
        return self.__alpha

    @alpha.setter
    def alpha(self, value: int) -> None:
        self.__alpha = value

    @property
    def k(self) -> int:
        return self.__k

    @k.setter
    def k(self, value: int) -> None:
        self.__k = value

    @property
    def message_find_node_timeout(self) -> int:
        return self.__message_find_node_timeout

    @message_find_node_timeout.setter
    def message_find_node_timeout(self, value: int) -> None:
        self.__message_find_node_timeout = value

    @property
    def message_ping_node_timeout(self) -> int:
        return self.__message_ping_node_timeout

    @message_ping_node_timeout.setter
    def message_ping_node_timeout(self, value: int) -> None:
        self.__message_ping_node_timeout = value
