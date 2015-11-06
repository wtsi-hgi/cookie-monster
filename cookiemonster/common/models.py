from datetime import date, MINYEAR
from typing import Union


class FileUpdate:
    """
    Model of a file update.
    """
    def __init__(self, file_location: str, file_hash: str, timestamp: date):
        """
        Constructor.
        :param file_location: the location of the file that has been updated
        :param file_hash: hash of the file
        :param timestamp: the timestamp of when the file was updated
        :return:
        """
        self.file_location = file_location
        self.file_hash = file_hash
        self.timestamp = timestamp


class FileUpdateCollection(list):
    def get_most_recent(self) -> Union[None, FileUpdate]:
        """
        Gets the file update in the collection with the most recent timestamp.

        O(n) operation.
        :return: the file update in the collection with the most recent timestamp
        """
        if len(self) == 0:
            return None

        most_recent = FileUpdate("", "", date(MINYEAR, 1, 1))
        for file_update in self:
            if file_update.timestamp > most_recent.timestamp:
                most_recent = file_update

        return most_recent
