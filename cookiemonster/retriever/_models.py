from datetime import datetime

from hgicommon.models import Model


class RetrievalLog(Model):
    """
    Model of a log of a update retrieval.
    """
    def __init__(self, started_at: datetime, seconds_taken_to_complete_query: float,
                 number_of_updates: int, latest_retrieved_timestamp: datetime):
        """
        Constructor.
        :param started_at: the time when the retrieval started
        :param seconds_taken_to_complete_query: the time taken to complete the retrieval query in seconds
        :param number_of_updates: the number of updates updates retrieved
        :param latest_retrieved_timestamp: the timestamp of the most recent update
        """
        self.started_at = started_at
        self.seconds_taken_to_complete_query = seconds_taken_to_complete_query
        self.number_of_updates = number_of_updates
        self.latest_retrieved_timestamp = latest_retrieved_timestamp
