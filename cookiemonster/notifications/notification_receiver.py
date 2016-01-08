from abc import ABCMeta, abstractmethod

from hgicommon.data_source import RegisteringDataSource

from cookiemonster import Notification


class NotificationReceiver(metaclass=ABCMeta):
    """
    Receiver of notifications.
    """
    @abstractmethod
    def receive(self, notification: Notification):
        """
        Receive notification.
        :param notification: the notification to give to the receiver
        """
        pass



class NotificationReceiverLoaderSource(RegisteringDataSource):
    """
    Notification receiver source where `NotificationReceiver` are registered from within Python modules within a given
    directory. These modules can be changed on-the-fly.
    """
    # Regex used to determine if a file contains an enrichment loader(s)
    FILE_PATH_MATCH_REGEX = ".*loader\.py"
    _COMPILED_FILE_PATH_MATCH_REGEX = re.compile(FILE_PATH_MATCH_REGEX)

    def __init__(self, directory_location: str):
        """
        Constructor.
        :param directory_location: the directory in which enrichment loaders can be sourced from
        """
        super().__init__(directory_location, EnrichmentLoader)

    def is_data_file(self, file_path: str) -> bool:
        return EnrichmentLoaderSource._COMPILED_FILE_PATH_MATCH_REGEX.search(file_path)
