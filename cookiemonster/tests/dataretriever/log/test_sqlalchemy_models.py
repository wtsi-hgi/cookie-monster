import unittest

from datetime import date, timedelta

from cookiemonster.dataretriever._models import RetrievalLog
from cookiemonster.dataretriever.log.sqlalchemy_models import SQLAlchemyRetrievalLog


class TestSQLAlchemyRetrievalLog(unittest.TestCase):
    _LATEST_RETRIEVED_TIMESTAMP = date.max
    _NUMBER_OF_FILE_UPDATES = 74
    _TIME_TAKEN_TO_COMPLETE_QUERY = timedelta(seconds=3)

    def setUp(self):
        sqlalchemy_retrieve_log = SQLAlchemyRetrievalLog()
        sqlalchemy_retrieve_log.latest_retrieved_timestamp = TestSQLAlchemyRetrievalLog._LATEST_RETRIEVED_TIMESTAMP
        sqlalchemy_retrieve_log.number_of_file_updates = TestSQLAlchemyRetrievalLog._NUMBER_OF_FILE_UPDATES
        sqlalchemy_retrieve_log.time_taken_to_complete_query = TestSQLAlchemyRetrievalLog._TIME_TAKEN_TO_COMPLETE_QUERY
        self._sqlalchemy_retrieve_log = sqlalchemy_retrieve_log
        self._retrieve_log = RetrievalLog(
            TestSQLAlchemyRetrievalLog._LATEST_RETRIEVED_TIMESTAMP,
            TestSQLAlchemyRetrievalLog._NUMBER_OF_FILE_UPDATES,
            TestSQLAlchemyRetrievalLog._TIME_TAKEN_TO_COMPLETE_QUERY)

    def test_value_of(self):
        sqlalchemy_retrieve_log = SQLAlchemyRetrievalLog.value_of(self._retrieve_log)   # type: SQLAlchemyRetrievalLog
        self.assertEquals(sqlalchemy_retrieve_log.latest_retrieved_timestamp, TestSQLAlchemyRetrievalLog._LATEST_RETRIEVED_TIMESTAMP)
        self.assertEquals(sqlalchemy_retrieve_log.number_of_file_updates, TestSQLAlchemyRetrievalLog._NUMBER_OF_FILE_UPDATES)
        self.assertEquals(sqlalchemy_retrieve_log.time_taken_to_complete_query, TestSQLAlchemyRetrievalLog._TIME_TAKEN_TO_COMPLETE_QUERY)

    def test_to_retrieval_log(self):
        retrieve_log = self._sqlalchemy_retrieve_log.to_retrieval_log()
        self.assertEquals(retrieve_log.latest_retrieved_timestamp, TestSQLAlchemyRetrievalLog._LATEST_RETRIEVED_TIMESTAMP)
        self.assertEquals(retrieve_log.number_of_file_updates, TestSQLAlchemyRetrievalLog._NUMBER_OF_FILE_UPDATES)
        self.assertEquals(retrieve_log.time_taken_to_complete_query, TestSQLAlchemyRetrievalLog._TIME_TAKEN_TO_COMPLETE_QUERY)




if __name__ == '__main__':
    unittest.main()
