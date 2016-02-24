import tempfile
import unittest
from datetime import datetime, timedelta

from sqlalchemy import create_engine

from cookiemonster.common.sqlalchemy import SQLAlchemyDatabaseConnector
from cookiemonster.retriever._models import RetrievalLog
from cookiemonster.retriever.log.sqlalchemy_mapper import SQLAlchemyRetrievalLogMapper
from cookiemonster.retriever.log.sqlalchemy_models import SQLAlchemyModel



_STARTED_AT = datetime.min
_SECONDS_TAKEN_TO_COMPLETE_QUERY = 3.0
_NUMBER_OF_FILE_UPDATES = 74
_LATEST_RETRIEVED_TIMESTAMP = datetime.max
_SECONDS_TAKEN_TO_COMPLETE_QUERY = 0.1


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
        retrieve_log = RetrievalLog(
            _STARTED_AT, _SECONDS_TAKEN_TO_COMPLETE_QUERY, _NUMBER_OF_FILE_UPDATES, _LATEST_RETRIEVED_TIMESTAMP)
        self._mapper.add(retrieve_log)
        self.assertEqual(self._mapper.get_most_recent(), retrieve_log)

    def test_get_most_recent(self):
        retrieve_logs = [
            RetrievalLog(_STARTED_AT, _SECONDS_TAKEN_TO_COMPLETE_QUERY, _NUMBER_OF_FILE_UPDATES, datetime(10, 10, 10)),
            RetrievalLog(_STARTED_AT, _SECONDS_TAKEN_TO_COMPLETE_QUERY, _NUMBER_OF_FILE_UPDATES, datetime(20, 10, 10)),
            RetrievalLog(_STARTED_AT, _SECONDS_TAKEN_TO_COMPLETE_QUERY, _NUMBER_OF_FILE_UPDATES, datetime(5, 10, 10))]
        for retrieve_log in retrieve_logs:
            self._mapper.add(retrieve_log)
        self.assertEqual(self._mapper.get_most_recent(), retrieve_logs[-1])

    def test_get_most_recent_when_no_entries(self):
        self.assertEqual(self._mapper.get_most_recent(), None)


if __name__ == "__main__":
    unittest.main()
