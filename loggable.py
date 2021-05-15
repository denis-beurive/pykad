from typing import Dict, Any
from abc import ABC, abstractmethod


class Loggable(ABC):

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """
        Generate a key/value pairs representation of the message.
        :return: a dictionary that represents the message.
        """
        pass
