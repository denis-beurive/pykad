from threading import Lock
from typing import TextIO, Optional
from message.message import Message, MessageAction
import json


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

    @staticmethod
    def log_message(message: Message, action: MessageAction, tag: Optional[str] = None) -> None:
        with Logger.__lock:
            d = message.to_dict()
            d['action'] = action.value
            if tag is None:
                Logger.__fd.write(json.dumps(d))
            else:
                Logger.__fd.write("# {}\n{}\n".format(tag, json.dumps(d)))

    @staticmethod
    def log_data(data: str, tag: Optional[str] = None) -> None:
        with Logger.__lock:
            if tag is None:
                Logger.__fd.write(data)
            else:
                Logger.__fd.write("# {}\n{}\n".format(tag, data))
