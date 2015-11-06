from abc import ABCMeta, abstractmethod
from datetime import datetime

from cookiemonster.dataretriever._models import QueryResult


class FileUpdateRetriever(metaclass=ABCMeta):
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