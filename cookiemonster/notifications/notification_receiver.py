import re
from typing import Callable

from hgicommon.data_source import RegisteringDataSource

from cookiemonster import Notification


class NotificationReceiver:
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


class NotificationReceiverSource(RegisteringDataSource):
    """
    Notification receiver source where `NotificationReceiver` are registered from within Python modules within a given
    directory. These modules can be changed on-the-fly.
    """
    # Regex used to determine if a file contains an enrichment loader(s)
    FILE_PATH_MATCH_REGEX = ".*notification_receiver\.py"
    _COMPILED_FILE_PATH_MATCH_REGEX = re.compile(FILE_PATH_MATCH_REGEX)

    def __init__(self, directory_location: str):
        """
        Constructor.
        :param directory_location: the directory in which notification receivers can be sourced from
        """
        super().__init__(directory_location, NotificationReceiver)

    def is_data_file(self, file_path: str) -> bool:
        return NotificationReceiverSource._COMPILED_FILE_PATH_MATCH_REGEX.search(file_path)
