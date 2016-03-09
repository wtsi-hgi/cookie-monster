from abc import abstractmethod, ABCMeta
from datetime import datetime

from cookiemonster.common.collections import UpdateCollection


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
