from abc import ABCMeta, abstractmethod

from typing import List, Callable

from cookiemonster.common.models import FileUpdate, Notification
from cookiemonster.rulesengine._collections import RuleProcessingQueue


class DataProcessor(metaclass=ABCMeta):
    """
    Processes a single file update.
    """
    @abstractmethod
    def process_file_update(
            self, file_update: FileUpdate,
            rule_queue: RuleProcessingQueue,
            on_complete: Callable[[List[Notification]], None]):
        """
        Processes the given file update.
        :param file_update: the file update to process
        :param rule_queue: the rules to use when processing the file update
        :param on_complete: the on complete method that must be called when the processing has completed
        """
        pass


class DataProcessorManager(metaclass=ABCMeta):
    """
    Manages the processing of file updates.
    """
    @abstractmethod
    def on_new_file_update(self, file_update: FileUpdate):
        """
        Called when a new file update has been received.
        :param file_update: the file that has been updated
        """
        pass

    @abstractmethod
    def on_file_update_processed(self, file_update: FileUpdate, notifications: List[Notification]):
        """
        Called when a file update has been processed.
        :param file_update: the file update that has been processed
        :param notifications: list of external processes that are to be notified
        """
        pass
