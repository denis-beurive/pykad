from threading import Lock


class Uid:

    __uid = 0
    __lock = Lock()

    @staticmethod
    def uid() -> int:
        with Uid.__lock:
            Uid.__uid += 1
            return Uid.__uid
