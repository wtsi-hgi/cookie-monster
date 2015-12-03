from datetime import timedelta, datetime

from hgicommon.models import Model

from cookiemonster.common.collections import FileUpdateCollection


class RetrievalLog(Model):
    """
    Model of a log of a file update retrieval.
    """
    def __init__(self, retrieved_file_updates_since: datetime, number_of_file_updates: int,
                 time_taken_to_complete_query: timedelta):
        """
        Constructor.
        :param retrieved_file_updates_since: the timestamp of since when files were retrieved from
        :param number_of_file_updates: the number of file updates retrieved
        :param time_taken_to_complete_query: the time taken to complete the retrieval query
        """
        self.retrieved_file_updates_since = retrieved_file_updates_since
        self.number_of_file_updates = number_of_file_updates
        self.time_taken_to_complete_query = time_taken_to_complete_query

    def __hash__(self) -> hash:
        return hash(self.retrieved_file_updates_since)


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
