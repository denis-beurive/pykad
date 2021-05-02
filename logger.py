from threading import RLock
from typing import TextIO, Optional
from message.message import Message, MessageAction
import json


class Logger:

    __lock_fd: RLock = RLock()
    __shared_fd: TextIO = None

    @staticmethod
    def init(path: str) -> None:
        Logger.__shared_fd = open(path, "w")

    @staticmethod
    def log(message: str) -> None:
        with Logger.__lock_fd:
            Logger.__shared_fd.write(message + "\n")

    @staticmethod
    def log_message(message: Message, action: MessageAction, tag: Optional[str] = None) -> None:
        with Logger.__lock_fd:
            d = message.to_dict()
            d['action'] = action.value
            if tag is None:
                Logger.__shared_fd.write(json.dumps(d))
            else:
                Logger.__shared_fd.write("# {}\n{}\n".format(tag, json.dumps(d)))

    @staticmethod
    def log_data(data: str, tag: Optional[str] = None) -> None:
        with Logger.__lock_fd:
            if tag is None:
                Logger.__shared_fd.write(data)
            else:
                Logger.__shared_fd.write("# {}\n{}\n".format(tag, data))
