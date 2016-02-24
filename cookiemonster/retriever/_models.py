from datetime import datetime

from hgicommon.models import Model


class RetrievalLog(Model):
    """
    Model of a log of a update retrieval.
    """
    def __init__(self, started_at: datetime, seconds_taken_to_complete_query: float,
                 number_of_updates: int, retrieved_updates_since: datetime):
        """
        Constructor.
        :param started_at: the time when the retrieval started
        :param seconds_taken_to_complete_query: the time taken to complete the retrieval query in seconds
        :param number_of_updates: the number of updates updates retrieved
        :param retrieved_updates_since: the timestamp of since when updates were retrieved from
        """
        self.started_at = started_at
        self.seconds_taken_to_complete_query = seconds_taken_to_complete_query
        self.number_of_updates = number_of_updates
        self.retrieved_updates_since = retrieved_updates_since
