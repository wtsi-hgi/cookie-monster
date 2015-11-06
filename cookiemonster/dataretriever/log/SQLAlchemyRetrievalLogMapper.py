from cookiemonster.dataretriever._models import RetrievalLog


class SQLAlchemyRetrievalLogMapper:
    """
    TODO.
    """
    def add(self, log: RetrievalLog):
        raise NotImplementedError()

    def get_most_recent(self) -> RetrievalLog:
        raise NotImplementedError()