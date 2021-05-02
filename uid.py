from threading import Lock


class Uid:

    __shared_uid = 0
    __lock_uid = Lock()

    @staticmethod
    def uid() -> int:
        with Uid.__lock_uid:
            Uid.__shared_uid += 1
            return Uid.__shared_uid
