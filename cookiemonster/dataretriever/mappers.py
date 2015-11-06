from abc import abstractmethod, ABCMeta

from cookiemonster.dataretriever._models import RetrievalLog


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
