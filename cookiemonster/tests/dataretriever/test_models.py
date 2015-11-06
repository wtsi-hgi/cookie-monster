import unittest

from datetime import date
from datetime import timedelta

from cookiemonster.dataretriever._models import RetrievalLog


class TestRetrievalLog(unittest.TestCase):
    _LATEST_RETRIEVED_TIMESTAMP = date.max
    _NUMBER_OF_FILE_UPDATES = 74
    _TIME_TAKEN_TO_COMPLETE_QUERY = timedelta(seconds=3)

    def setUp(self):
        self._retrieval_log = RetrievalLog(TestRetrievalLog._LATEST_RETRIEVED_TIMESTAMP,
                                           TestRetrievalLog._NUMBER_OF_FILE_UPDATES,
                                           TestRetrievalLog._TIME_TAKEN_TO_COMPLETE_QUERY)

    def test_equal_non_nullity(self):
        self.assertNotEqual(self._retrieval_log, None)

    def test_equal_different_type(self):
        self.assertNotEqual(self._retrieval_log, date)

    def test_equal_reflexivity(self):
        model = self._retrieval_log
        self.assertEqual(model, model)

    def test_equal_symmetry(self):
        model1 = self._retrieval_log
        model2 = self._retrieval_log
        self.assertEqual(model1, model2)
        self.assertEqual(model2, model1)

    def test_equal_transitivity(self):
        model1 = self._retrieval_log
        model2 = self._retrieval_log
        model3 = self._retrieval_log
        self.assertEqual(model1, model2)
        self.assertEqual(model2, model3)
        self.assertEqual(model1, model3)

    def test_hash_equal_if_equal(self):
        model1 = self._retrieval_log
        model2 = self._retrieval_log
        self.assertEquals(hash(model1), hash(model2))

    def test_can_get_string_representation(self):
        string_representation = str(self._retrieval_log)
        self.assertTrue(isinstance(string_representation, str))