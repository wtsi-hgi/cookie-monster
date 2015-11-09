from datetime import timedelta, datetime

from cookiemonster.common.models import FileUpdateCollection, Model


class RetrievalLog(Model):
    """
    Model of a log .
    """
    def __init__(self, latest_retrieved_timestamp: datetime, number_of_file_updates: int,
                 time_taken_to_complete_query: timedelta):
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

    def __hash__(self) -> hash:
        return hash(self.latest_retrieved_timestamp)


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
