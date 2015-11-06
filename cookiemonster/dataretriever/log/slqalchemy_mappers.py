from cookiemonster.common.sqlalchemy_database_connector import SQLAlchemyDatabaseConnector
from cookiemonster.dataretriever._models import RetrievalLog
from cookiemonster.dataretriever.log.sqlalchemy_models import SQLAlchemyRetrievalLog


class SQLAlchemyRetrievalLogMapper:
    """
    Data mapper for `RetrievalLogMapper` that is implemented using SQLAlchemy.
    """
    def __init__(self, database_connector: SQLAlchemyDatabaseConnector):
        """
        Constructor.
        :param database_connector: the database connector
        """
        self._database_connector = database_connector

    def add(self, retrieval_log: RetrievalLog):
        session = self._database_connector.create_session()
        session.add(SQLAlchemyRetrievalLog.value_of(retrieval_log))
        session.commit()
        session.close()

    def get_most_recent(self) -> RetrievalLog:
        session = self._database_connector.create_session()
        result = session.query(SQLAlchemyRetrievalLog).\
            order_by(SQLAlchemyRetrievalLog.latest_retrieved_timestamp.desc()).first()  # type: SQLAlchemyRetrievalLog
        session.close()
        return result.to_retrieval_log()