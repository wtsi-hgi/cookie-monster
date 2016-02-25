import unittest
from datetime import datetime

from cookiemonster.retriever._models import RetrievalLog
from cookiemonster.retriever.log._sqlalchemy_converters import convert_to_retrieval_log, \
    convert_to_sqlalchemy_retrieval_log
from cookiemonster.retriever.log.sqlalchemy_models import SQLAlchemyRetrievalLog

_STARTED_AT = datetime.min
_SECONDS_TAKEN_TO_COMPLETE_QUERY = 3.0
_NUMBER_OF_FILE_UPDATES = 74
_LATEST_RETRIEVED_TIMESTAMP = datetime.max



class TestConvertToRetrievalLog(unittest.TestCase):
    """
    Tests for `convert_to_retrieval_log` method.
    """
    def setUp(self):
        sqlalchemy_retrieve_log = SQLAlchemyRetrievalLog()
        sqlalchemy_retrieve_log.started_at = _STARTED_AT
        sqlalchemy_retrieve_log.seconds_taken_to_complete_query = _SECONDS_TAKEN_TO_COMPLETE_QUERY
        sqlalchemy_retrieve_log.number_of_file_updates = _NUMBER_OF_FILE_UPDATES
        sqlalchemy_retrieve_log.latest_retrieved_timestamp = _LATEST_RETRIEVED_TIMESTAMP

        self._sqlalchemy_retrieve_log = sqlalchemy_retrieve_log

    def test_with_valid(self):
        retrieve_log = convert_to_retrieval_log(self._sqlalchemy_retrieve_log)
        self.assertEqual(retrieve_log.started_at, _STARTED_AT)
        self.assertEqual(retrieve_log.seconds_taken_to_complete_query, _SECONDS_TAKEN_TO_COMPLETE_QUERY)
        self.assertEqual(retrieve_log.number_of_updates, _NUMBER_OF_FILE_UPDATES)
        self.assertEqual(retrieve_log.latest_retrieved_timestamp, _LATEST_RETRIEVED_TIMESTAMP)


class TestConvertToSqlalchemyRetrievalLog(unittest.TestCase):
    """
    Tests for `convert_to_sqlalchemy_retrieval_log` method.
    """
    def setUp(self):
        self._retrieve_log = RetrievalLog(_STARTED_AT, _SECONDS_TAKEN_TO_COMPLETE_QUERY,
                                          _NUMBER_OF_FILE_UPDATES, _LATEST_RETRIEVED_TIMESTAMP)

    def test_with_valid(self):
        sqlalchemy_retrieve_log = convert_to_sqlalchemy_retrieval_log(self._retrieve_log)
        self.assertEqual(sqlalchemy_retrieve_log.started_at, _STARTED_AT)
        self.assertEqual(sqlalchemy_retrieve_log.seconds_taken_to_complete_query, _SECONDS_TAKEN_TO_COMPLETE_QUERY)
        self.assertEqual(sqlalchemy_retrieve_log.number_of_file_updates, _NUMBER_OF_FILE_UPDATES)
        self.assertEqual(sqlalchemy_retrieve_log.latest_retrieved_timestamp, _LATEST_RETRIEVED_TIMESTAMP)


if __name__ == "__main__":
    unittest.main()
