from abc import ABCMeta, abstractmethod
from datetime import timedelta, datetime
from typing import Tuple

from cookiemonster.common.models import FileUpdateCollection
from cookiemonster.dataretriever.models import RetrievalLog


class QueryResult:
    """
    TODO.
    """
    def __init__(self, file_updates: FileUpdateCollection, time_taken_to_complete_query: timedelta):
        """
        TODO
        :param file_updates:
        :param time_taken_to_complete_query:
        :return:
        """
        self.file_updates = file_updates
        self.time_taken_to_complete_query = time_taken_to_complete_query


class RetrievalLogMapper(metaclass=ABCMeta):
    """
    TODO.
    """
    @abstractmethod
    def add(self, log: RetrievalLog):
        pass

    @abstractmethod
    def get_most_recent(self) -> RetrievalLog:
        pass


class FileUpdateRetriever(maetaclass=ABCMeta):
    """
    Base class for classes that retrieve data regarding file updates.
    """
    @abstractmethod
    def query_for_all_file_updates_since(self, since: datetime) -> QueryResult:
        """
        Gets models of all of the file updates that have happened since the given time.
        :param since: the time at which to get updates from (`fileUpdate.timestamp > since`)
        :return: TODO
        """
        pass