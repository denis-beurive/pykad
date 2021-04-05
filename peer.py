
class Peer:

    def __init__(self, identifier: int, last_seen: int):
        self.__identifier: int = identifier
        self.__last_seen: int = last_seen

    @property
    def identifier(self) -> int:
        return self.__identifier

    @identifier.setter
    def identifier(self, value: int) -> None:
        self.__identifier = value

    @property
    def last_seen(self) -> int:
        return self.__last_seen

    @last_seen.setter
    def last_seen(self, timestamp: int) -> None:
        self.__last_seen = timestamp

    def __str__(self) -> str:
        return '(id:{0:d}, tt:{1:d})'.format(self.__identifier, self.__last_seen)

