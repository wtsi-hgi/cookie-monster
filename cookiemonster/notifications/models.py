from typing import Callable

from cookiemonster import Notification
from cookiemonster.common.resource_accessor import ResourceAccessor


class NotificationReceiver(ResourceAccessor):
    """
    Receiver of notifications.
    """
    def __init__(self, receive: Callable[[Notification], None]):
        """
        Constructor.
        :param receive: method proxied by `receive`
        """
        self._receive = receive

    def receive(self, notification: Notification):
        """
        Receive notification. It is this method's responsibility to determine if it is interested in the given
        notification (the cookie monster informs all notification receivers about all notifications).
        :param notification: the notification to give to the receiver
        """
        self._receive(notification)
