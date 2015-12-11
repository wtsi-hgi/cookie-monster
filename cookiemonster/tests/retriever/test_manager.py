import unittest
from datetime import timedelta, date, datetime
from threading import Thread
from typing import Any
from unittest.mock import MagicMock, call

from hgicommon.collections import Metadata

from cookiemonster.common.collections import FileUpdateCollection
from cookiemonster.common.models import Update
from cookiemonster.retriever._models import QueryResult, RetrievalLog
from cookiemonster.retriever.manager import PeriodicRetrievalManager, RetrievalManager
from cookiemonster.tests.retriever._stubs import StubFileUpdateRetriever
from cookiemonster.tests.retriever._stubs import StubRetrievalLogMapper


class _MockRetrievalManager(RetrievalManager):
    """
    RetrievalManager that does a single retrieve when ran.
    """
    def run(self, file_updates_since: datetime = date.min):
        self._do_retrieve(file_updates_since)


class _BaseRetrievalManagerTest(unittest.TestCase):
    """
    Base class for unit tests on `RetrievalManager` instances.
    """
    SINCE = datetime.min
    TIME_TAKEN_TO_DO_RETRIEVE = timedelta(0)

    def setUp(self):
        # Create dependencies
        self._file_update_retriever = StubFileUpdateRetriever()
        self._retrieval_log_mapper = StubRetrievalLogMapper()

        # Create retrieval manager
        self._retrieval_manager = _MockRetrievalManager(self._file_update_retriever, self._retrieval_log_mapper)

        # Setup mock FileUpdateRetriever
        self._file_updates = FileUpdateCollection([
            Update("a", hash("b"), datetime(year=1999, month=1, day=2), Metadata()),
            Update("b", hash("c"), datetime(year=1998, month=12, day=20), Metadata())])
        self._query_result = QueryResult(self._file_updates, _BaseRetrievalManagerTest.TIME_TAKEN_TO_DO_RETRIEVE)
        self._file_update_retriever.query_for_all_file_updates_since = MagicMock(return_value=self._query_result)


class TestRetrievalManager(_BaseRetrievalManagerTest):
    """
    Test cases for `RetrievalManager`.
    """
    def test__do_retrieve_with_file_updates(self):
        # Setup
        listener = MagicMock()
        self._retrieval_manager.add_listener(listener)
        self._retrieval_log_mapper.add = MagicMock()

        # Call SUT method
        self._retrieval_manager._do_retrieve(_BaseRetrievalManagerTest.SINCE)

        # Assert that retrieves updates from source
        self._file_update_retriever.query_for_all_file_updates_since.assert_called_once_with(
            _BaseRetrievalManagerTest.SINCE)
        # Assert that updates listener
        listener.assert_called_once_with(self._file_updates)
        # Assert that retrieval is logged
        self._retrieval_log_mapper.add.assert_called_once_with(
            RetrievalLog(_BaseRetrievalManagerTest.SINCE, len(self._file_updates),
                         _BaseRetrievalManagerTest.TIME_TAKEN_TO_DO_RETRIEVE))

    def test__do_retrieve_without_file_updates(self):
        # Setup
        listener = MagicMock()
        self._retrieval_manager.add_listener(listener)
        self._retrieval_log_mapper.add = MagicMock()
        self._file_updates.clear()
        self._retrieval_manager._retrieved_file_updates_since = \
            TestPeriodicRetrievalManager.CURRENT_TIME - timedelta(days=1)

        # Call SUT method
        self._retrieval_manager._do_retrieve(_BaseRetrievalManagerTest.SINCE)

        # Assert that retrieves updates from source
        self._file_update_retriever.query_for_all_file_updates_since.assert_called_once_with(_BaseRetrievalManagerTest.SINCE)
        # Assert that updates listener has not been called given that there are no file updates
        listener.assert_not_called()
        # Assert that retrieval is logged but that latest retrieved timestamp has not target
        self._retrieval_log_mapper.add.assert_called_once_with(
            RetrievalLog(_BaseRetrievalManagerTest.SINCE, len(self._file_updates), _BaseRetrievalManagerTest.TIME_TAKEN_TO_DO_RETRIEVE))


class TestPeriodicRetrievalManager(_BaseRetrievalManagerTest):
    """
    Test cases for `PeriodicRetrievalManager`.
    """
    RETRIEVAL_PERIOD = timedelta(milliseconds=1)
    CURRENT_TIME = datetime(year=2000, month=2, day=1)

    def setUp(self):
        super(TestPeriodicRetrievalManager, self).setUp()

        # Create retrieval manager
        self._retrieval_manager = PeriodicRetrievalManager(
            TestPeriodicRetrievalManager.RETRIEVAL_PERIOD, self._file_update_retriever, self._retrieval_log_mapper)

        # Override current time to make deterministic
        PeriodicRetrievalManager._get_current_time = MagicMock(return_value=TestPeriodicRetrievalManager.CURRENT_TIME)

        def do_query(*args):
            PeriodicRetrievalManager._get_current_time = MagicMock(
                return_value=PeriodicRetrievalManager._get_current_time()
                             + TestPeriodicRetrievalManager.RETRIEVAL_PERIOD
                             + self._query_result.time_taken_to_complete_query)
            return self._query_result

        self._file_update_retriever.query_for_all_file_updates_since = MagicMock(side_effect=do_query)

    def test_run(self):
        max_cycles = 10
        self._setup_to_do_only_n_cycles(max_cycles)

        self._retrieval_log_mapper.add = MagicMock()

        listener = MagicMock()
        self._retrieval_manager.add_listener(listener)

        self._retrieval_manager.run(_BaseRetrievalManagerTest.SINCE)

        self.assertEquals(self._retrieval_log_mapper.add.call_count, max_cycles)
        listener.assert_has_calls([call(self._file_updates) for _ in range(max_cycles)])

    def test_run_if_running(self):
        Thread(target=self._retrieval_manager.run).start()
        self.assertRaises(RuntimeError, self._retrieval_manager.run)
        self._retrieval_manager.stop()

    def test_start_if_started(self):
        self._retrieval_manager.start()
        self.assertRaises(RuntimeError, self._retrieval_manager.start)
        self._retrieval_manager.stop()

    def _setup_to_do_only_n_cycles(self, number_of_cycles: int):
        """
        Sets up the test so that the retriever will only do n cycles.
        :param number_of_cycles: the number of cycles to do
        """
        counter = 0
        original_schedule_next_periodic_retrieve = self._retrieval_manager._schedule_next_retrieve

        def limit_cycles(*args, **kargs):
            nonlocal counter, original_schedule_next_periodic_retrieve
            counter += 1
            if counter >= number_of_cycles:
                self._retrieval_manager._schedule_next_retrieve = MagicMock()
                original_schedule_next_periodic_retrieve = MagicMock()
                self._retrieval_manager.stop()

        def extended_schedule_next_periodic_retrieve(*args, **kargs) -> Any:
            nonlocal original_schedule_next_periodic_retrieve
            limit_cycles(*args, **kargs)
            original_schedule_next_periodic_retrieve(*args, **kargs)

        self._retrieval_manager._schedule_next_retrieve = extended_schedule_next_periodic_retrieve


if __name__ == "__main__":
    unittest.main()
