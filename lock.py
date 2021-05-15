from typing import Optional, Union
from threading import Lock, RLock


class BaseLock(object):
    """
    Base class for all extended locks.
    """

    def __init__(self, in_lock: Union[Lock, RLock], in_resource: Optional[str] = None):
        """
        Create a new instance of BaseLock.

        :param in_lock: the lock, which is an instance of threading.Lock or threading.RLock.
        :param in_resource: the name of the lock protected resource.
        """
        self.__locker: Optional[str] = None
        self.__resource: Optional[str] = in_resource
        self._lock: Union[Lock, RLock] = in_lock

    @property
    def locker(self) -> Optional[str]:
        """
        The identifier of the entity that acquires the lock.
        """
        return self.__locker

    @property
    def resource(self) -> Optional[str]:
        """
        The name of the lock protected resource.
        """
        return self.__resource

    def set(self, locker: str, resource: Optional[str] = None):
        """
        Set the identifier of the entity that acquires the lock, and,
        optionally, the name of the lock protected resource.

        Typical usage:

            resource = {}
            lock = ExtLock("protected resource")
            with lock.set("main"):
                resource[1] = 2

        :param locker: the identifier of the entity that acquires the lock.
        :param resource: the name of the lock protected resource.
        :return: the instance of BaseLock.
        """

        self.__locker = locker
        if resource is not None:
            self.__resource = resource
        return self

    def acquire(self):
        print("acquired", self)
        self._lock.acquire()

    def release(self):
        print("released", self)
        self._lock.release()

    def __enter__(self):
        print('"{}" acquires "{}"'.format(self.locker if self.locker is not None else "locker is not set",
                                          self.resource if self.resource is not None else "resource is not set"))
        self.acquire()

    def __exit__(self, type, value, traceback):
        print('"{}" releases "{}"'.format(self.locker if self.locker is not None else "locker is not set",
                                          self.resource if self.resource is not None else "resource is not set"))
        self.release()


class ExtLock(BaseLock):

    def __init__(self, in_resource: Optional[str] = None):
        super().__init__(Lock(), in_resource)


class ExtRLock(BaseLock):

    def __init__(self, in_resource: Optional[str] = None):
        super().__init__(RLock(), in_resource)



