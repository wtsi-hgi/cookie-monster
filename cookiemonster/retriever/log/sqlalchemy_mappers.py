from cookiemonster.common.sqlalchemy_database_connector import SQLAlchemyDatabaseConnector
from cookiemonster.retriever._models import RetrievalLog
from cookiemonster.retriever.log._sqlalchemy_converters import convert_to_sqlalchemy_retrieval_log, \
    convert_to_retrieval_log
from cookiemonster.retriever.log._sqlalchemy_models import SQLAlchemyRetrievalLog
from cookiemonster.retriever.mappers import RetrievalLogMapper


class SQLAlchemyRetrievalLogMapper(RetrievalLogMapper):
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
        session.add(convert_to_sqlalchemy_retrieval_log(retrieval_log))
        session.commit()
        session.close()

    def get_most_recent(self) -> RetrievalLog:
        session = self._database_connector.create_session()
        result = session.query(SQLAlchemyRetrievalLog).\
            order_by(SQLAlchemyRetrievalLog.id.desc()).first()  # type: SQLAlchemyRetrievalLog
        session.close()
        return convert_to_retrieval_log(result)
