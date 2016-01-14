from datetime import datetime

from cookiemonster.common.collections import UpdateCollection
from cookiemonster.retriever._models import RetrievalLog
from cookiemonster.retriever.mappers import RetrievalLogMapper, UpdateMapper


class StubRetrievalLogMapper(RetrievalLogMapper):
    """
    Stub of `RetrievalLogMapper`.
    """
    def add(self, retrieval_log: RetrievalLog):
        pass

    def get_most_recent(self) -> RetrievalLog:
        pass


class StubUpdateMapper(UpdateMapper):
    """
    Stub of `UpdateMapper`.
    """
    def get_all_since(self, since: datetime) -> UpdateCollection:
        return []
