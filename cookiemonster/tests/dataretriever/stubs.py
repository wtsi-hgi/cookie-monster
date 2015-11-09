from datetime import datetime

from cookiemonster.dataretriever._models import RetrievalLog, QueryResult
from cookiemonster.dataretriever._retriever import FileUpdateRetriever
from cookiemonster.dataretriever.mappers import RetrievalLogMapper


class StubRetrievalLogMapper(RetrievalLogMapper):
    """
    Stub of `RetrievalLogMapper`.
    """
    def add(self, retrieval_log: RetrievalLog):
        pass

    def get_most_recent(self) -> RetrievalLog:
        pass


class StubFileUpdateRetriever(FileUpdateRetriever):
    """
    Stub of `FileUpdateRetriever`.
    """
    def query_for_all_file_updates_since(self, since: datetime) -> QueryResult:
        pass
