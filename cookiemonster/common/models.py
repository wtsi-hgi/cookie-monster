from datetime import date


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
