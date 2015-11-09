from datetime import timedelta, datetime

from cookiemonster.common.collections import FileUpdateCollection
from cookiemonster.common.models import Model


class RetrievalLog(Model):
    """
    Model of a log of a file update retrieval.
    """
    def __init__(self, latest_retrieved_timestamp: datetime, number_of_file_updates: int,
                 time_taken_to_complete_query: timedelta):
        """
        Constructor.
        :param latest_retrieved_timestamp: the timestamp of the most recent file update that was retrieved
        :param number_of_file_updates: the number of file updates retrieved
        :param time_taken_to_complete_query: the time taken to complete the retrieval query
        """
        self.latest_retrieved_timestamp = latest_retrieved_timestamp
        self.number_of_file_updates = number_of_file_updates
        self.time_taken_to_complete_query = time_taken_to_complete_query

    def __hash__(self) -> hash:
        return hash(self.latest_retrieved_timestamp)


class QueryResult(Model):
    """
    Model of a query result.
    """
    def __init__(self, file_updates: FileUpdateCollection, time_taken_to_complete_query: timedelta):
        """
        Constructor.
        :param file_updates: the collection of files that have been updated
        :param time_taken_to_complete_query: the time taken for the query to complete
        """
        self.file_updates = file_updates
        self.time_taken_to_complete_query = time_taken_to_complete_query
