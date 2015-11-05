from abc import ABCMeta, abstractmethod
from datetime import date
from typing import List

from cookiemonster.common.models import FileUpdate


class FileUpdateRetriever(maetaclass=ABCMeta):
    """
    Base class for classes that retrieve data regarding file updates
    """
    @abstractmethod
    def get_all_since(self, since: date) -> List[FileUpdate]:
        """
        Gets models of all of the file updates that have happened since the given time.
        :param since: the time at which to get updates from (`fileUpdate.timestamp > since`)
        :return: an unordered list of all the files that have been updated since the given time
        """
        pass