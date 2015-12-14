from abc import abstractmethod, ABCMeta
from datetime import datetime

from cookiemonster.common.collections import UpdateCollection
from cookiemonster.retriever._models import RetrievalLog


class RetrievalLogMapper(metaclass=ABCMeta):
    """
    A data mapper as defined by Martin Fowler (see: http://martinfowler.com/eaaCatalog/dataMapper.html) that moves data
    between objects and a the retrieval log database, while keeping them independent of each other and the mapper
    itself.
    """
    @abstractmethod
    def add(self, retrieval_log: RetrievalLog):
        """
        Adds a retrieval retrieval_log to the database.
        :param retrieval_log: the log to store in the database
        """
        pass

    @abstractmethod
    def get_most_recent(self) -> RetrievalLog:
        """
        Gets the most recent retrieval log that was added to the database.
        :return: the most recently added retrieval log
        """
        pass


class UpdateMapper(metaclass=ABCMeta):
    """
    Retrieves information about updates from a data source.
    """
    @abstractmethod
    def get_all_since(self, since: datetime) -> UpdateCollection:
        """
        Gets models of all of the updates that have happened since the given time.
        :param since: the time at which to get updates from (`fileUpdate.timestamp > since`)
        :return: the results of the query
        """
        pass
