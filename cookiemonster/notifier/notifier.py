from abc import ABCMeta, abstractmethod

from cookiemonster.common.models import Notification


class Notifier(metaclass=ABCMeta):
    @abstractmethod
    def do(self, notification: Notification):
        pass

