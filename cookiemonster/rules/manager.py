from abc import ABCMeta
from abc import abstractmethod

from typing import List

from cookiemonster.common.models import FileUpdate, Notification


class ProcessorManager(metaclass=ABCMeta):
    """
    Manages the continuous processing of file updates.
    """
    @abstractmethod
    def on_information(self):
        """
        Called when a new file update has been received.

        TODO: Better name?
        """
        pass

    @abstractmethod
    def on_processed(self, file_update: FileUpdate, notifications: List[Notification]):
        """
        Called when processing of a job has been completed
        :param file_update: the file update that has been processed
        :param notifications: list of external processes that are to be notified. List should be empty if no decision
        could be made with the known information
        """
        pass
