from datetime import datetime
from typing import List

from cookiemonster.common.models import FileUpdate


class FileUpdateCollection(list):
    """
    Collection of `FileUpdate` instances. Extends built-in `list`.
    """
    def get_most_recent(self) -> List[FileUpdate]:
        """
        Gets the file updates in the collection with the most recent timestamp.

        O(n) operation.
        :return: the file updates in the collection with the most recent timestamp
        """
        if len(self) == 0:
            raise ValueError("No file updates in collection")

        most_recent = [FileUpdate("", "", datetime.min, Metadata())]
        for file_update in self:
            assert len(most_recent) > 0
            most_recent_so_far = most_recent[0].timestamp
            if file_update.timestamp > most_recent_so_far:
                most_recent.clear()
            if file_update.timestamp >= most_recent_so_far:
                most_recent.append(file_update)

        return most_recent


class Metadata(dict):
    """
    Self-canonicalising dictionary for metadata
    """
    def __init__(self, base=None, **kwargs):
        """
        Override constructor, so base and kwargs are canonicalised
        """
        super(Metadata, self).__init__(**kwargs)
        if base and type(base) is dict:
            for key, value in base.items():
                self.__setitem__(key, value)

        for key, value in kwargs.items():
            self.__setitem__(key, value)

    def __setitem__(self, key, value):
        """
        Override __setitem__, so scalar values are put into a list and
        lists are sorted and made unique

        n.b., We assume our dictionaries are only one deep
        """
        if type(value) is list:
            super().__setitem__(key, sorted(set(value)))
        else:
            super().__setitem__(key, [value])
