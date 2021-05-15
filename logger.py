from typing import TextIO, Optional
from message.message import Message, MessageAction
import json
from lock import ExtRLock
from kad_config import KadConfig
from kad_types import NodeId


class Logger:

    __lock_fd = ExtRLock("Logger.fd")
    __shared_fd: TextIO = None

    @staticmethod
    def init(path: str) -> None:
        Logger.__shared_fd = open(path, "w")

    @staticmethod
    def log(message: str) -> None:
        with Logger.__lock_fd.set("logger.Logger.log"):
            Logger.__shared_fd.write(message + "\n")

    @staticmethod
    def log_message(message: Message, action: MessageAction, tag: Optional[str] = None) -> None:
        with Logger.__lock_fd.set("logger.Logger.log_message"):
            d = message.to_dict()
            d['action'] = action.value
            if tag is None:
                Logger.__shared_fd.write(json.dumps(d) + "\n")
            else:
                Logger.__shared_fd.write("# {}\n{}\n".format(tag, json.dumps(d)))

    @staticmethod
    def log_data(data: str, tag: Optional[str] = None) -> None:
        with Logger.__lock_fd.set("logger.Logger.log_data"):
            if tag is None:
                Logger.__shared_fd.write(data + "\n")
            else:
                Logger.__shared_fd.write("# {}\n{}\n".format(tag, data))

    @staticmethod
    def log_rt(node_id: NodeId, rt, tag: Optional[str] = None) -> None:
        # Note: cannot use typing hint (because of circular reference).
        with Logger.__lock_fd.set("logger.Logger.log_config"):
            d = rt.to_dict()
            d["node_id"] = node_id
            if tag is None:
                Logger.__shared_fd.write(json.dumps(d) + "\n")
            else:
                Logger.__shared_fd.write("# {}\n{}\n".format(tag, json.dumps(d)))

    @staticmethod
    def log_config(config: KadConfig, tag: Optional[str] = None) -> None:
        d = config.to_dict()
        with Logger.__lock_fd.set("logger.Logger.log_config"):
            if tag is None:
                Logger.__shared_fd.write(json.dumps(d) + "\n")
            else:
                Logger.__shared_fd.write("# {}\n{}\n".format(tag, json.dumps(d)))
