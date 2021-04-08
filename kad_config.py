
class KadConfig:

    def __init__(self, list_size=20, id_length=128, alpha=3):
        self.__bucket_size: int = list_size
        self.__id_length: int = id_length
        self.__alpha: int = alpha

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
    def alpha(self, value: int):
        self.__alpha = value
