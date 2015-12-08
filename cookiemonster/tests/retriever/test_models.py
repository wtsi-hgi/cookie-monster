import unittest
from datetime import date
from datetime import timedelta

from cookiemonster.retriever._models import RetrievalLog


class TestRetrievalLog(unittest.TestCase):
    """
    Tests for `RetrievalLog`.
    """
    _LATEST_RETRIEVED_TIMESTAMP = date.max
    _NUMBER_OF_FILE_UPDATES = 74
    _TIME_TAKEN_TO_COMPLETE_QUERY = timedelta(seconds=3)

    def setUp(self):
        self._retrieval_log = RetrievalLog(TestRetrievalLog._LATEST_RETRIEVED_TIMESTAMP,
                                           TestRetrievalLog._NUMBER_OF_FILE_UPDATES,
                                           TestRetrievalLog._TIME_TAKEN_TO_COMPLETE_QUERY)

    def test_hash_equal_if_equal(self):
        model1 = self._retrieval_log
        model2 = self._retrieval_log
        self.assertEquals(hash(model1), hash(model2))

    def test_hash_not_equal_if_not_equal_latest_retrieved_timestamp(self):
        model1 = self._retrieval_log
        model2 = RetrievalLog(
            TestRetrievalLog._LATEST_RETRIEVED_TIMESTAMP - TestRetrievalLog._TIME_TAKEN_TO_COMPLETE_QUERY,
            TestRetrievalLog._NUMBER_OF_FILE_UPDATES,
            TestRetrievalLog._TIME_TAKEN_TO_COMPLETE_QUERY)
        self.assertEquals(hash(model1), hash(model2))

    def test_hash_not_equal_if_not_equal_number_of_file_updates(self):
        model1 = self._retrieval_log
        model2 = RetrievalLog(
            TestRetrievalLog._LATEST_RETRIEVED_TIMESTAMP,
            TestRetrievalLog._NUMBER_OF_FILE_UPDATES + 5,
            TestRetrievalLog._TIME_TAKEN_TO_COMPLETE_QUERY)
        self.assertEquals(hash(model1), hash(model2))

    def test_hash_not_equal_if_not_equal_time_taken_to_complete_query(self):
        model1 = self._retrieval_log
        model2 = RetrievalLog(
            TestRetrievalLog._LATEST_RETRIEVED_TIMESTAMP,
            TestRetrievalLog._NUMBER_OF_FILE_UPDATES,
            TestRetrievalLog._TIME_TAKEN_TO_COMPLETE_QUERY + timedelta(seconds=5))
        self.assertEquals(hash(model1), hash(model2))


if __name__ == "__main__":
    unittest.main()
