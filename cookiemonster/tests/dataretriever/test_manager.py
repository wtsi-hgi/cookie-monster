import unittest
from datetime import timedelta, date, datetime
from unittest.mock import MagicMock

from cookiemonster.common.collections import FileUpdateCollection
from cookiemonster.dataretriever._models import QueryResult
from cookiemonster.dataretriever.manager import RetrievalManager
from cookiemonster.tests.dataretriever.stubs import StubFileUpdateRetriever
from cookiemonster.tests.dataretriever.stubs import StubRetrievalLogMapper


class TestRetrievalManager(unittest.TestCase):
    """
    Test cases for `RetrievalManager`.
    """
    _TIME_DELTA = timedelta(seconds=10)
    _DATE_TIME = date.min
    _CURRENT_TIME = datetime(year=2000, month=2, day=1)

    def setUp(self):
        self._retrieval_period = TestRetrievalManager._TIME_DELTA
        self._file_update_retriever = StubFileUpdateRetriever()
        self._retrieval_log_mapper = StubRetrievalLogMapper()
        self._retrieval_manager = RetrievalManager(
            self._retrieval_period, self._file_update_retriever, self._retrieval_log_mapper)

        self._file_updates = FileUpdateCollection()
        self._query_result = QueryResult(self._file_updates, TestRetrievalManager._TIME_DELTA)
        self._file_update_retriever.query_for_all_file_updates_since = MagicMock(return_value=self._query_result)

        # Disable timer
        self._retrieval_manager._set_timer_for_next_periodic_retrieve = MagicMock()
        # Override current time
        RetrievalManager._get_current_time = MagicMock(return_value=TestRetrievalManager._CURRENT_TIME)

    def test__do_retrieve(self):
        listener = MagicMock()
        self._retrieval_manager.add_listener(listener)
        self._retrieval_manager._do_retrieve(TestRetrievalManager._DATE_TIME)

        self._retrieval_log_mapper.add = MagicMock()

        # Check that retrieves updates from source
        self._file_update_retriever.query_for_all_file_updates_since.assert_called_once_with(
            TestRetrievalManager._DATE_TIME)
        # Check that updates listener
        listener.assert_called_once_with(self._file_updates)
        # FIXME: Check that retrieval is logged
        # self._retrieval_log_mapper.add.assert_called_once_with(
        #     RetrievalLog(TestRetrievalManager._CURRENT_TIME, len(self._file_updates), TestRetrievalManager._TIME_DELTA))

    def test__do_retrieve_periodically(self):
        self._retrieval_manager._do_retrieve = MagicMock()
        retrieval_scheduled_for = TestRetrievalManager._DATE_TIME

        self._retrieval_manager._do_retrieve_periodically(retrieval_scheduled_for)

        self._retrieval_manager._do_retrieve.assert_called_once_with(retrieval_scheduled_for)
        self._retrieval_manager._set_timer_for_next_periodic_retrieve.assert_called_once_with(
            retrieval_scheduled_for + self._retrieval_period)

    def test__log_retrieval(self):
        self._retrieval_manager._log_retrieval(self._query_result)
        self._retrieval_log_mapper.add = MagicMock()
        # self._retrieval_log_mapper.
        # FIXME: Complete when log mapper interface is done

    def test_start(self):
        self._retrieval_manager.start()
        self._retrieval_manager._set_timer_for_next_periodic_retrieve.assert_called_once_with(
            TestRetrievalManager._CURRENT_TIME)


if __name__ == '__main__':
    unittest.main()
