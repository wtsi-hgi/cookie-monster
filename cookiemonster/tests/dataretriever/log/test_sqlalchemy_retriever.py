import tempfile
import unittest
from datetime import datetime, timedelta

from sqlalchemy import create_engine

from cookiemonster.common.sqlalchemy_database_connector import SQLAlchemyDatabaseConnector
from cookiemonster.dataretriever._models import RetrievalLog
from cookiemonster.dataretriever.log.sqlalchemy_mappers import SQLAlchemyRetrievalLogMapper
from cookiemonster.dataretriever.log.sqlalchemy_models import SQLAlchemyModel


class TestSQLAlchemyRetrievalLogMapper(unittest.TestCase):
    """
    Tests for `SQLAlchemyRetrievalLogMapper`.
    """
    def setUp(self):
        file_handle, database_location = tempfile.mkstemp()
        database_url = "sqlite:///%s" % database_location
        engine = create_engine(database_url)
        SQLAlchemyModel.metadata.create_all(bind=engine)
        database_connector = SQLAlchemyDatabaseConnector(database_url)
        self._mapper = SQLAlchemyRetrievalLogMapper(database_connector)

    def test_add(self):
        retrieve_log = RetrievalLog(datetime(10, 10, 10), 1, timedelta.resolution)
        self._mapper.add(retrieve_log)
        self.assertEqual(self._mapper.get_most_recent(), retrieve_log)

    def test_get_most_recent(self):
        retrieve_logs = [
            RetrievalLog(datetime(10, 10, 10), 1, timedelta.resolution),
            RetrievalLog(datetime(20, 10, 10), 1, timedelta.resolution),
            RetrievalLog(datetime(5, 10, 10), 1, timedelta.resolution)]
        for retrieve_log in retrieve_logs:
            self._mapper.add(retrieve_log)
        self.assertEqual(self._mapper.get_most_recent(), retrieve_logs[1])


if __name__ == '__main__':
    unittest.main()
