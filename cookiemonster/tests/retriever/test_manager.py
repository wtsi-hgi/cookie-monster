import unittest
from datetime import timedelta, date, datetime
from unittest.mock import MagicMock, call

from cookiemonster.common.collections import FileUpdateCollection
from cookiemonster.common.models import FileUpdate
from cookiemonster.retriever._models import QueryResult, RetrievalLog
from cookiemonster.retriever.manager import RetrievalManager
from cookiemonster.tests.retriever.stubs import StubFileUpdateRetriever
from cookiemonster.tests.retriever.stubs import StubRetrievalLogMapper


class TestRetrievalManager(unittest.TestCase):
    """
    Test cases for `RetrievalManager`.
    """
    _TIME_TAKEN_TO_DO_RETRIEVE = timedelta(seconds=10)
    _RETRIEVAL_PERIOD = timedelta(seconds=5)
    _SINCE = date.min
    _CURRENT_TIME = datetime(year=2000, month=2, day=1)

    def setUp(self):
        # Create dependencies
        self._file_update_retriever = StubFileUpdateRetriever()
        self._retrieval_log_mapper = StubRetrievalLogMapper()

        # Create retrieval manager
        self._retrieval_manager = RetrievalManager(
            TestRetrievalManager._RETRIEVAL_PERIOD, self._file_update_retriever, self._retrieval_log_mapper)

        # Setup mock FileUpdateRetriever
        self._file_updates = FileUpdateCollection([
            FileUpdate("a", hash("b"), datetime(year=1999, month=1, day=2)),
            FileUpdate("b", hash("c"), datetime(year=1998, month=12, day=20))])
        self._query_result = QueryResult(self._file_updates, TestRetrievalManager._TIME_TAKEN_TO_DO_RETRIEVE)
        self._file_update_retriever.query_for_all_file_updates_since = MagicMock(return_value=self._query_result)

        # Force retrieval manager to only do one cycle
        def do_one_cycle(*args):
            self._retrieval_manager._schedule_next_periodic_retrieve.side_effect = MagicMock()
            self._retrieval_manager._do_retrieve_periodically(*args)
        self._retrieval_manager._schedule_next_periodic_retrieve = MagicMock(side_effect=do_one_cycle)

        # Override current time to make deterministic
        RetrievalManager._get_current_time = MagicMock(return_value=TestRetrievalManager._CURRENT_TIME)

    def test__do_retrieve_with_file_updates(self):
        # Setup
        listener = MagicMock()
        self._retrieval_manager.add_listener(listener)
        self._retrieval_log_mapper.add = MagicMock()

        # Call SUT method
        self._retrieval_manager._do_retrieve(TestRetrievalManager._SINCE)

        # Assert that retrieves updates from source
        self._file_update_retriever.query_for_all_file_updates_since.assert_called_once_with(
            TestRetrievalManager._SINCE)
        # Assert that updates listener
        listener.assert_called_once_with(self._file_updates)
        # Assert that retrieval is logged
        self._retrieval_log_mapper.add.assert_called_once_with(
            RetrievalLog(
                self._file_updates.get_most_recent()[0].timestamp, len(self._file_updates),
                TestRetrievalManager._TIME_TAKEN_TO_DO_RETRIEVE)
        )

    def test__do_retrieve_without_file_updates(self):
        # Setup
        listener = MagicMock()
        self._retrieval_manager.add_listener(listener)
        self._retrieval_log_mapper.add = MagicMock()
        self._file_updates.clear()
        self._retrieval_manager._latest_retrieved_timestamp = TestRetrievalManager._CURRENT_TIME - timedelta(days=1)

        # Call SUT method
        self._retrieval_manager._do_retrieve(TestRetrievalManager._SINCE)

        # Assert that retrieves updates from source
        self._file_update_retriever.query_for_all_file_updates_since.assert_called_once_with(
            TestRetrievalManager._SINCE)
        # Assert that updates listener has not been called given that there are no file updates
        listener.assert_not_called()
        # Assert that retrieval is logged but that latest retrieved timestamp has not changed
        self._retrieval_log_mapper.add.assert_called_once_with(
            RetrievalLog(
                self._retrieval_manager._latest_retrieved_timestamp, len(self._file_updates),
                TestRetrievalManager._TIME_TAKEN_TO_DO_RETRIEVE)
        )

    def test__do_retrieve_periodically(self):
        # Setup
        self._retrieval_manager._do_retrieve = MagicMock()
        retrieval_scheduled_for = TestRetrievalManager._SINCE
        self._retrieval_manager._schedule_next_periodic_retrieve = MagicMock()

        # Call SUT method
        self._retrieval_manager._do_retrieve_periodically(retrieval_scheduled_for)

        # Assert that _do_retrieve was called
        self._retrieval_manager._do_retrieve.assert_called_once_with(retrieval_scheduled_for)
        # Assert that next cycle was scheduled
        self._retrieval_manager._schedule_next_periodic_retrieve.assert_called_once_with(
            retrieval_scheduled_for + TestRetrievalManager._RETRIEVAL_PERIOD)

    def test_start(self):
        # Call SUT method
        self._retrieval_manager.start()

        # Assert started periodic retrieval for file updates since the correct time
        self._retrieval_manager._schedule_next_periodic_retrieve.assert_has_calls([
            call(TestRetrievalManager._CURRENT_TIME),
            call(TestRetrievalManager._CURRENT_TIME + TestRetrievalManager._RETRIEVAL_PERIOD)
        ], any_order=False)


if __name__ == '__main__':
    unittest.main()
