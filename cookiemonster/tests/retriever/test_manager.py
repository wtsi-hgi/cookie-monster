import logging
import unittest
from datetime import datetime
from threading import Thread, Semaphore, Lock
from unittest.mock import MagicMock, call

from hgicommon.collections import Metadata

from cookiemonster.common.collections import UpdateCollection
from cookiemonster.common.helpers import localise_to_utc
from cookiemonster.common.models import Update
from cookiemonster.retriever.manager import PeriodicRetrievalManager, RetrievalManager
from cookiemonster.tests.retriever._stubs import StubUpdateMapper, StubRetrievalLogMapper


class _BaseRetrievalManagerTest(unittest.TestCase):
    """
    Base class for unit tests on `RetrievalManager` instances.
    """
    SINCE = localise_to_utc(datetime.min)
    TIME_TAKEN_TO_DO_RETRIEVE = 1.0

    def setUp(self):
        self.update_mapper = StubUpdateMapper()
        self.retrieval_log_mapper = StubRetrievalLogMapper()

        self.updates = UpdateCollection([
            Update("a", datetime(year=1999, month=1, day=2), Metadata()),
            Update("b", datetime(year=1998, month=12, day=20), Metadata())])

        def do_query(*args):
            RetrievalManager._get_current_time = MagicMock(
                return_value=RetrievalManager._get_current_time() + TestPeriodicRetrievalManager.RETRIEVAL_PERIOD
                             + _BaseRetrievalManagerTest.TIME_TAKEN_TO_DO_RETRIEVE)
            return self.updates

        self.update_mapper.get_all_since = MagicMock(side_effect=do_query)

        logging.root.setLevel(level=logging.ERROR)


class TestRetrievalManager(_BaseRetrievalManagerTest):
    """
    Test cases for `RetrievalManager`.
    """
    def setUp(self):
        super().setUp()

        # Create retrieval manager
        self.retrieval_manager = RetrievalManager(self.update_mapper, self.retrieval_log_mapper)

    def test_run_with_updates(self):
        # Setup
        listener = MagicMock()
        self.retrieval_manager.add_listener(listener)
        self.retrieval_log_mapper.add = MagicMock()

        # Call SUT method
        self.retrieval_manager.run(_BaseRetrievalManagerTest.SINCE)

        # Assert that retrieves updates from source
        self.update_mapper.get_all_since.assert_called_once_with(_BaseRetrievalManagerTest.SINCE)
        # Assert that updates listener
        listener.assert_called_once_with(self.updates)
        # Assert that retrieval is logged
        self.assertEqual(self.retrieval_log_mapper.add.call_count, 1)
        retrieval_log = self.retrieval_log_mapper.add.call_args[0][0]  # type: RetrievalLog
        self.assertEqual(retrieval_log.retrieved_updates_since, _BaseRetrievalManagerTest.SINCE)
        self.assertEqual(retrieval_log.number_of_updates, len(self.updates))
        self.assertGreaterEqual(
                retrieval_log.seconds_taken_to_complete_query, _BaseRetrievalManagerTest.TIME_TAKEN_TO_DO_RETRIEVE)

    def test_run_without_updates(self):
        # Setup
        listener = MagicMock()
        self.retrieval_manager.add_listener(listener)
        self.retrieval_log_mapper.add = MagicMock()
        self.updates.clear()
        self.retrieval_manager._retrieved_updates_since = TestPeriodicRetrievalManager.CURRENT_TIME - (24 * 60 * 60)

        # Call SUT method
        self.retrieval_manager.run(_BaseRetrievalManagerTest.SINCE)

        # Assert that retrieves updates from source
        self.update_mapper.get_all_since.assert_called_once_with(_BaseRetrievalManagerTest.SINCE)
        # Assert that updates listener has not been called given that there are no updates
        listener.assert_not_called()
        # Assert that retrieval is logged but that latest retrieved timestamp has not changed
        self.assertEqual(self.retrieval_log_mapper.add.call_count, 1)
        retrieval_log = self.retrieval_log_mapper.add.call_args[0][0]  # type: RetrievalLog
        self.assertEqual(retrieval_log.retrieved_updates_since, _BaseRetrievalManagerTest.SINCE)
        self.assertEqual(retrieval_log.number_of_updates, len(self.updates))
        self.assertGreaterEqual(
                retrieval_log.seconds_taken_to_complete_query, _BaseRetrievalManagerTest.TIME_TAKEN_TO_DO_RETRIEVE)


class TestPeriodicRetrievalManager(_BaseRetrievalManagerTest):
    """
    Test cases for `PeriodicRetrievalManager`.
    """
    RETRIEVAL_PERIOD = 0.0001
    CURRENT_TIME = 0

    def setUp(self):
        super().setUp()

        # Create retrieval manager
        self.retrieval_manager = PeriodicRetrievalManager(
            TestPeriodicRetrievalManager.RETRIEVAL_PERIOD, self.update_mapper, self.retrieval_log_mapper)

    def test_run(self):
        cycles = 10
        listener = MagicMock()

        self.retrieval_log_mapper.add = MagicMock()
        self.retrieval_manager.add_listener(listener)

        self._setup_to_do_n_cycles(cycles, self.updates)

        self.assertEqual(self.retrieval_log_mapper.add.call_count, cycles)
        listener.assert_has_calls([call(self.updates) for _ in range(cycles)])

    def test_run_if_running(self):
        Thread(target=self.retrieval_manager.run).start()
        self.assertRaises(RuntimeError, self.retrieval_manager.run)

    def test_stop_and_then_restart(self):
        self.retrieval_manager.start()
        self.retrieval_manager.stop()
        self.retrieval_manager.start()

    def _setup_to_do_n_cycles(self, number_of_cycles: int, updates_each_cycle: UpdateCollection=None):
        """
        Sets up the test so that the retriever will only do n cycles.
        :param number_of_cycles: the number of cycles to do
        """
        if updates_each_cycle is None:
            updates_each_cycle = UpdateCollection([])

        semaphore = Semaphore(0)
        lock_until_counted = Lock()
        lock_until_counted.acquire()

        def increase_counter(*args) -> UpdateCollection:
            semaphore.release()
            lock_until_counted.acquire()
            return updates_each_cycle

        self.retrieval_manager.update_mapper.get_all_since.side_effect = increase_counter
        self.retrieval_manager.start()

        run_counter = 0
        while run_counter < number_of_cycles:
            semaphore.acquire()
            run_counter += 1
            lock_until_counted.release()
            if run_counter == number_of_cycles:
                self.retrieval_manager.stop()

        self.retrieval_manager.update_mapper.get_all_since.side_effect = None

    def tearDown(self):
        self.retrieval_manager.stop()


if __name__ == "__main__":
    unittest.main()
