import unittest
from datetime import date, timedelta

from cookiemonster.retriever._models import RetrievalLog
from cookiemonster.retriever.log._sqlalchemy_converters import convert_to_retrieval_log, \
    convert_to_sqlalchemy_retrieval_log
from cookiemonster.retriever.log._sqlalchemy_models import SQLAlchemyRetrievalLog


_LATEST_RETRIEVED_TIMESTAMP = date.max
_NUMBER_OF_FILE_UPDATES = 74
_TIME_TAKEN_TO_COMPLETE_QUERY = timedelta(seconds=3)


class TestConvertToRetrievalLog(unittest.TestCase):
    """
    Tests for `convert_to_retrieval_log` method.
    """
    def setUp(self):
        sqlalchemy_retrieve_log = SQLAlchemyRetrievalLog()
        sqlalchemy_retrieve_log.latest_retrieved_timestamp = _LATEST_RETRIEVED_TIMESTAMP
        sqlalchemy_retrieve_log.number_of_file_updates = _NUMBER_OF_FILE_UPDATES
        sqlalchemy_retrieve_log.time_taken_to_complete_query = _TIME_TAKEN_TO_COMPLETE_QUERY
        self._sqlalchemy_retrieve_log = sqlalchemy_retrieve_log

    def test_with_valid(self):
        retrieve_log = convert_to_retrieval_log(self._sqlalchemy_retrieve_log)
        self.assertEquals(retrieve_log.retrieved_updates_since, _LATEST_RETRIEVED_TIMESTAMP)
        self.assertEquals(retrieve_log.number_of_updates, _NUMBER_OF_FILE_UPDATES)
        self.assertEquals(retrieve_log.time_taken_to_complete_query, _TIME_TAKEN_TO_COMPLETE_QUERY)


class TestConvertToSqlalchemyRetrievalLog(unittest.TestCase):
    """
    Tests for `convert_to_sqlalchemy_retrieval_log` method.
    """
    def setUp(self):
        self._retrieve_log = RetrievalLog(
            _LATEST_RETRIEVED_TIMESTAMP,
            _NUMBER_OF_FILE_UPDATES,
            _TIME_TAKEN_TO_COMPLETE_QUERY)

    def test_with_valid(self):
        sqlalchemy_retrieve_log = convert_to_sqlalchemy_retrieval_log(self._retrieve_log)
        self.assertEquals(sqlalchemy_retrieve_log.latest_retrieved_timestamp, _LATEST_RETRIEVED_TIMESTAMP)
        self.assertEquals(sqlalchemy_retrieve_log.number_of_file_updates, _NUMBER_OF_FILE_UPDATES)
        self.assertEquals(sqlalchemy_retrieve_log.time_taken_to_complete_query, _TIME_TAKEN_TO_COMPLETE_QUERY)


if __name__ == "__main__":
    unittest.main()
