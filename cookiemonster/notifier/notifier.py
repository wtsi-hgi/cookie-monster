from abc import ABCMeta, abstractmethod

from cookiemonster.common.models import Notification


class Notifier(metaclass=ABCMeta):
    """
    TODO
    """
    @abstractmethod
    def do(self, notification: Notification):
        """
        TODO
        :param notification:
        :return:
        """
        pass

