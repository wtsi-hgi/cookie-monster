from datetime import timedelta, datetime

from cookiemonster.common.models import FileUpdateCollection, Model


class RetrievalLog(Model):
    """
    TODO.
    """
    def __init__(self, latest_retrieved_timestamp: datetime, number_of_file_updates: int, time_taken_to_complete_query: timedelta):
        """
        TODO
        :param latest_retrieved_timestamp:
        :param number_of_file_updates:
        :param time_taken_to_complete_query:
        :return:
        """
        self.number_of_file_updates = number_of_file_updates
        self.time_taken_to_complete_query = time_taken_to_complete_query
        self.latest_retrieved_timestamp = latest_retrieved_timestamp

    def __eq__(self, other) -> bool:
        if not isinstance(other, self.__class__):
            return False
        if self.number_of_file_updates == other.number_of_file_updates and \
                        self.time_taken_to_complete_query == other.time_taken_to_complete_query and \
                        self.latest_retrieved_timestamp == other.latest_retrieved_timestamp:
            return True




class QueryResult(Model):
    """
    TODO.
    """
    def __init__(self, file_updates: FileUpdateCollection, time_taken_to_complete_query: timedelta):
        """
        TODO
        :param file_updates:
        :param time_taken_to_complete_query:
        :return:
        """
        self.file_updates = file_updates
        self.time_taken_to_complete_query = time_taken_to_complete_query