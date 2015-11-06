from datetime import datetime

from cookiemonster.dataretriever._models import RetrievalLog, QueryResult
from cookiemonster.dataretriever._retriever import FileUpdateRetriever
from cookiemonster.dataretriever.mappers import RetrievalLogMapper


class StubRetrievalLogMapper(RetrievalLogMapper):
    """
    TODO.
    """
    def add(self, log: RetrievalLog):
        pass

    def get_most_recent(self) -> RetrievalLog:
        pass


class StubFileUpdateRetriever(FileUpdateRetriever):
    """
    TODO
    """
    def query_for_all_file_updates_since(self, since: datetime) -> QueryResult:
        """
        Gets models of all of the file updates that have happened since the given time.
        :param since: the time at which to get updates from (`fileUpdate.timestamp > since`)
        :return: TODO
        """
        pass
