from abc import ABCMeta, abstractmethod

from cookiemonster.common.models import Notification


class Notifier(metaclass=ABCMeta):
    """
    Notifies interested party about notifications generated from processing cookies.
    """
    @abstractmethod
    def do(self, notification: Notification):
        """
        Notifies the interested part of the given notification.
        :param notification: the notification to give
        """
        pass
