import logging
import unittest
from datetime import timedelta, datetime
from threading import Thread
from typing import Any
from unittest.mock import MagicMock, call

from hgicommon.collections import Metadata

from cookiemonster.common.collections import UpdateCollection
from cookiemonster.common.models import Update
from cookiemonster.retriever._models import RetrievalLog
from cookiemonster.retriever.manager import PeriodicRetrievalManager, RetrievalManager
from cookiemonster.tests.retriever._stubs import StubUpdateMapper, StubRetrievalLogMapper


class _BaseRetrievalManagerTest(unittest.TestCase):
    """
    Base class for unit tests on `RetrievalManager` instances.
    """
    SINCE = datetime.min
    TIME_TAKEN_TO_DO_RETRIEVE = timedelta(milliseconds=1)

    def setUp(self):
        self.update_retriever = StubUpdateMapper()
        self.retrieval_log_mapper = StubRetrievalLogMapper()

        self.updates = UpdateCollection([
            Update("a", datetime(year=1999, month=1, day=2), Metadata()),
            Update("b", datetime(year=1998, month=12, day=20), Metadata())])

        def do_query(*args):
            RetrievalManager._get_current_time = MagicMock(
                return_value=RetrievalManager._get_current_time() + TestPeriodicRetrievalManager.RETRIEVAL_PERIOD
                             + _BaseRetrievalManagerTest.TIME_TAKEN_TO_DO_RETRIEVE)
            return self.updates

        self.update_retriever.get_all_since = MagicMock(side_effect=do_query)

        logging.root.setLevel(level=logging.ERROR)


class TestRetrievalManager(_BaseRetrievalManagerTest):
    """
    Test cases for `RetrievalManager`.
    """
    def setUp(self):
        super().setUp()

        # Create retrieval manager
        self.retrieval_manager = RetrievalManager(self.update_retriever, self.retrieval_log_mapper)

    def test_run_with_updates(self):
        # Setup
        listener = MagicMock()
        self.retrieval_manager.add_listener(listener)
        self.retrieval_log_mapper.add = MagicMock()

        # Call SUT method
        self.retrieval_manager.run(_BaseRetrievalManagerTest.SINCE)

        # Assert that retrieves updates from source
        self.update_retriever.get_all_since.assert_called_once_with(_BaseRetrievalManagerTest.SINCE)
        # Assert that updates listener
        listener.assert_called_once_with(self.updates)
        # Assert that retrieval is logged
        self.assertEquals(self.retrieval_log_mapper.add.call_count, 1)
        retrieval_log = self.retrieval_log_mapper.add.call_args[0][0]  # type: RetrievalLog
        self.assertEquals(retrieval_log.retrieved_updates_since, _BaseRetrievalManagerTest.SINCE)
        self.assertEquals(retrieval_log.number_of_updates, len(self.updates))
        self.assertGreaterEqual(
                retrieval_log.time_taken_to_complete_query, _BaseRetrievalManagerTest.TIME_TAKEN_TO_DO_RETRIEVE)

    def test_run_without_updates(self):
        # Setup
        listener = MagicMock()
        self.retrieval_manager.add_listener(listener)
        self.retrieval_log_mapper.add = MagicMock()
        self.updates.clear()
        self.retrieval_manager._retrieved_updates_since = TestPeriodicRetrievalManager.CURRENT_TIME - timedelta(days=1)

        # Call SUT method
        self.retrieval_manager.run(_BaseRetrievalManagerTest.SINCE)

        # Assert that retrieves updates from source
        self.update_retriever.get_all_since.assert_called_once_with(_BaseRetrievalManagerTest.SINCE)
        # Assert that updates listener has not been called given that there are no updates
        listener.assert_not_called()
        # Assert that retrieval is logged but that latest retrieved timestamp has not changed
        self.assertEquals(self.retrieval_log_mapper.add.call_count, 1)
        retrieval_log = self.retrieval_log_mapper.add.call_args[0][0]  # type: RetrievalLog
        self.assertEquals(retrieval_log.retrieved_updates_since, _BaseRetrievalManagerTest.SINCE)
        self.assertEquals(retrieval_log.number_of_updates, len(self.updates))
        self.assertGreaterEqual(
                retrieval_log.time_taken_to_complete_query, _BaseRetrievalManagerTest.TIME_TAKEN_TO_DO_RETRIEVE)


class TestPeriodicRetrievalManager(_BaseRetrievalManagerTest):
    """
    Test cases for `PeriodicRetrievalManager`.
    """
    RETRIEVAL_PERIOD = timedelta(milliseconds=2)
    CURRENT_TIME = datetime(year=2000, month=2, day=1)

    def setUp(self):
        super().setUp()

        # Create retrieval manager
        self.retrieval_manager = PeriodicRetrievalManager(
            TestPeriodicRetrievalManager.RETRIEVAL_PERIOD, self.update_retriever, self.retrieval_log_mapper)

    def test_run(self):
        max_cycles = 10
        self._setup_to_do_only_n_cycles(max_cycles)

        self.retrieval_log_mapper.add = MagicMock()

        listener = MagicMock()
        self.retrieval_manager.add_listener(listener)

        self.retrieval_manager.run(_BaseRetrievalManagerTest.SINCE)

        self.assertEquals(self.retrieval_log_mapper.add.call_count, max_cycles)
        listener.assert_has_calls([call(self.updates) for _ in range(max_cycles)])

    def test_run_if_running(self):
        Thread(target=self.retrieval_manager.run).start()
        self.assertRaises(RuntimeError, self.retrieval_manager.run)
        self.retrieval_manager.stop()

    def _setup_to_do_only_n_cycles(self, number_of_cycles: int):
        """
        Sets up the test so that the retriever will only do n cycles.
        :param number_of_cycles: the number of cycles to do
        """
        counter = 0
        original_schedule_next_periodic_retrieve = self.retrieval_manager._schedule_next_retrieve

        def limit_cycles(*args, **kargs):
            nonlocal counter, original_schedule_next_periodic_retrieve
            counter += 1
            if counter >= number_of_cycles:
                self.retrieval_manager._schedule_next_retrieve = MagicMock()
                original_schedule_next_periodic_retrieve = MagicMock()
                self.retrieval_manager.stop()

        def extended_schedule_next_periodic_retrieve(*args, **kargs) -> Any:
            nonlocal original_schedule_next_periodic_retrieve
            limit_cycles(*args, **kargs)
            original_schedule_next_periodic_retrieve(*args, **kargs)

        self.retrieval_manager._schedule_next_retrieve = extended_schedule_next_periodic_retrieve


if __name__ == "__main__":
    unittest.main()
