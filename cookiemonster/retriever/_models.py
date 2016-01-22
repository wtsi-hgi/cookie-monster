from datetime import datetime

from hgicommon.models import Model


class RetrievalLog(Model):
    """
    Model of a log of a update retrieval.
    """
    def __init__(self, retrieved_updates_since: datetime, number_of_updates: int,
                 seconds_taken_to_complete_query: float):
        """
        Constructor.
        :param retrieved_updates_since: the timestamp of since when updates were retrieved from
        :param number_of_updates: the number of updates updates retrieved
        :param seconds_taken_to_complete_query: the time taken to complete the retrieval query in seconds
        """
        self.retrieved_updates_since = retrieved_updates_since
        self.number_of_updates = number_of_updates
        self.seconds_taken_to_complete_query = seconds_taken_to_complete_query

    def __hash__(self) -> hash:
        return hash(self.retrieved_updates_since)
