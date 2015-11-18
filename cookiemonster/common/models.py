from abc import ABCMeta
from datetime import date


class Model(metaclass=ABCMeta):
    """
    Abstract base class for models.
    """
    def __str__(self) -> str:
        string_builder = []
        for property, value in vars(self).items():
            string_builder.append("%s: %s" % (property, value))
        return "{ %s }" % ', '.join(string_builder)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        for property, value in vars(self).items():
            if other.__dict__[property] != self.__dict__[property]:
                return False
        return True


class FileUpdate(Model):
    """
    Model of a file update.
    """
    def __init__(self, file_location: str, file_hash: hash, timestamp: date):
        """
        Constructor.
        :param file_location: the location of the file that has been updated
        :param file_hash: hash of the file
        :param timestamp: the timestamp of when the file was updated
        """
        self.file_location = file_location
        self.file_hash = file_hash
        self.timestamp = timestamp
        # TODO: Add key/value pairs
