from datetime import timedelta, datetime

from hgicommon.models import Model


class RetrievalLog(Model):
    """
    Model of a log of a update retrieval.
    """
    def __init__(self, retrieved_updates_since: datetime, number_of_updates: int,
                 time_taken_to_complete_query: timedelta):
        """
        Constructor.
        :param retrieved_updates_since: the timestamp of since when updates were retrieved from
        :param number_of_updates: the number of updates updates retrieved
        :param time_taken_to_complete_query: the time taken to complete the retrieval query
        """
        self.retrieved_updates_since = retrieved_updates_since
        self.number_of_updates = number_of_updates
        self.time_taken_to_complete_query = time_taken_to_complete_query

    def __hash__(self) -> hash:
        return hash(self.retrieved_updates_since)
