from lock import ExtLock


class Uid:

    __shared_uid = 0
    __lock_uid = ExtLock("Uid.uid")

    @staticmethod
    def uid() -> int:
        with Uid.__lock_uid.set("uid.Uid.uid"):
            Uid.__shared_uid += 1
            return Uid.__shared_uid
