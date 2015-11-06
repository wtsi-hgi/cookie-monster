from datetime import date, MINYEAR
from typing import Union, List

class Model:
    def __str__(self) -> str:
        string_builder = []
        for property, value in vars(self).items():
            string_builder.append("%s: %s" % (property, value))
        return "{ %s }" % ', '.join(string_builder)


class FileUpdate(Model):
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
    def get_most_recent(self) -> List[FileUpdate]:
        """
        Gets the file updates in the collection with the most recent timestamp.

        O(n) operation.
        :return: the file updates in the collection with the most recent timestamp
        """
        if len(self) == 0:
            raise ValueError("No file updates in collection")

        most_recent = [FileUpdate("", "", date.min)]
        for file_update in self:
            assert len(most_recent) > 0
            most_recent_so_far = most_recent[0].timestamp
            if file_update.timestamp > most_recent_so_far:
                most_recent.clear()
            if file_update.timestamp >= most_recent_so_far:
                most_recent.append(file_update)

        return most_recent
