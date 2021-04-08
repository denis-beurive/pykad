
class KadConfig:

    def __init__(self, list_size=20, id_length=128, alpha=3):
        self.__list_size: int = list_size
        self.__id_length: int = id_length
        self.__alpha: int = alpha

    @property
    def list_size(self) -> int:
        return self.__list_size

    @list_size.setter
    def list_size(self, value: int) -> None:
        self.__list_size = value

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
    def alpha(self, value: int):
        self.__alpha = value
