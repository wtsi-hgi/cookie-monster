import unittest
from datetime import timedelta, date, datetime
from unittest.mock import MagicMock, call

from cookiemonster.common.collections import FileUpdateCollection
from cookiemonster.common.models import FileUpdate
from cookiemonster.dataretriever._models import QueryResult, RetrievalLog
from cookiemonster.dataretriever.manager import RetrievalManager
from cookiemonster.tests.dataretriever.stubs import StubFileUpdateRetriever
from cookiemonster.tests.dataretriever.stubs import StubRetrievalLogMapper


class TestRetrievalManager(unittest.TestCase):
    """
    Test cases for `RetrievalManager`.
    """
    _TIME_TAKEN_TO_DO_RETRIEVE = timedelta(seconds=10)
    _SINCE = date.min
    _CURRENT_TIME = datetime(year=2000, month=2, day=1)

    def setUp(self):
        self._retrieval_period = TestRetrievalManager._TIME_TAKEN_TO_DO_RETRIEVE
        self._file_update_retriever = StubFileUpdateRetriever()
        self._retrieval_log_mapper = StubRetrievalLogMapper()
        self._retrieval_manager = RetrievalManager(
            self._retrieval_period, self._file_update_retriever, self._retrieval_log_mapper)

        self._file_updates = FileUpdateCollection([
            FileUpdate("a", hash("b"), datetime(year=1999, month=1, day=2)),
            FileUpdate("b", hash("c"), datetime(year=1998, month=12, day=20))])
        self._query_result = QueryResult(self._file_updates, TestRetrievalManager._TIME_TAKEN_TO_DO_RETRIEVE)
        self._file_update_retriever.query_for_all_file_updates_since = MagicMock(return_value=self._query_result)

        # Do only one cycle
        def do_one_cycle(*args):
            self._retrieval_manager._set_timer_for_next_periodic_retrieve.side_effect = MagicMock()
            self._retrieval_manager._do_retrieve_periodically(*args)
        self._retrieval_manager._set_timer_for_next_periodic_retrieve = MagicMock(side_effect=do_one_cycle)
        # Override current time
        RetrievalManager._get_current_time = MagicMock(return_value=TestRetrievalManager._CURRENT_TIME)

    def test__do_retrieve(self):
        listener = MagicMock()
        self._retrieval_manager.add_listener(listener)
        self._retrieval_manager._do_retrieve(TestRetrievalManager._SINCE)

        self._retrieval_log_mapper.add = MagicMock()

        # Check that retrieves updates from source
        self._file_update_retriever.query_for_all_file_updates_since.assert_called_once_with(
            TestRetrievalManager._SINCE)
        # Check that updates listener
        listener.assert_called_once_with(self._file_updates)
        # FIXME: Check that retrieval is logged
        # self._retrieval_log_mapper.add.assert_called_once_with(
        #     RetrievalLog(TestRetrievalManager._CURRENT_TIME, len(self._file_updates), TestRetrievalManager._TIME_TAKEN_TO_DO_RETRIEVE))

    def test__do_retrieve_periodically(self):
        self._retrieval_manager._do_retrieve = MagicMock()
        retrieval_scheduled_for = TestRetrievalManager._SINCE
        # Stop cycles other than the one that is explicitly ran
        self._retrieval_manager._set_timer_for_next_periodic_retrieve = MagicMock()

        self._retrieval_manager._do_retrieve_periodically(retrieval_scheduled_for)

        self._retrieval_manager._do_retrieve.assert_called_once_with(retrieval_scheduled_for)
        self._retrieval_manager._set_timer_for_next_periodic_retrieve.assert_called_once_with(
            retrieval_scheduled_for + self._retrieval_period)

    def test__log_retrieval(self):
        self._retrieval_log_mapper.add = MagicMock()

        self._retrieval_manager.start()

        retrieval_log = RetrievalLog(self._file_updates.get_most_recent()[0].timestamp,
                             len(self._query_result.file_updates),
                             self._query_result.time_taken_to_complete_query)
        self._retrieval_log_mapper.add.assert_called_once_with(retrieval_log)

    def test_start(self):
        self._retrieval_manager.start()
        self._retrieval_manager._set_timer_for_next_periodic_retrieve.assert_has_calls([
            call(TestRetrievalManager._CURRENT_TIME),
            call(TestRetrievalManager._CURRENT_TIME + self._retrieval_period)
        ], any_order=False)

    # TODO: Test no retrieved with _do_retrieval


if __name__ == '__main__':
    unittest.main()
