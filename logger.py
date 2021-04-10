from threading import Lock
from typing import TextIO


class Logger:

    __lock: Lock = Lock()
    __fd: TextIO = None

    @staticmethod
    def init(path: str) -> None:
        Logger.__fd = open(path, "w")

    @staticmethod
    def log(message: str) -> None:
        with Logger.__lock:
            Logger.__fd.write(message + "\n")
