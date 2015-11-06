from cookiemonster.common.sqlalchemy_database_connector import SQLAlchemyDatabaseConnector
from cookiemonster.dataretriever._models import RetrievalLog


class SQLAlchemyRetrievalLogMapper:
    """
    TODO.
    """
    def __init__(self, database_connector: SQLAlchemyDatabaseConnector):
        """
        TODO
        :param database_connector:
        :return:
        """
        self._database_connector = database_connector

    def add(self, log: RetrievalLog):
        session = self._database_connector.create_session()
        result = session.query(query_model). \
            filter(query_model.is_current).all()
        session.close()
        assert isinstance(result, list)
        return convert_to_popo_models(result)

    def get_most_recent(self) -> RetrievalLog:
        raise NotImplementedError()