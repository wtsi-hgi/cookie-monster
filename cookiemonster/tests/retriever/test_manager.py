"""
Legalese
--------
Copyright (c) 2015, 2016 Genome Research Ltd.

Author: Colin Nolan <cn13@sanger.ac.uk>

This file is part of Cookie Monster.

Cookie Monster is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by the
Free Software Foundation; either version 3 of the License, or (at your
option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License along
with this program. If not, see <http://www.gnu.org/licenses/>.
"""
import logging
import unittest
from datetime import datetime
from threading import Thread, Semaphore, Lock
from typing import List
from unittest.mock import MagicMock, call

from hgicommon.collections import Metadata
from hgijson.json.primitive import DatetimeISOFormatJSONDecoder

from cookiemonster.common.collections import UpdateCollection
from cookiemonster.common.helpers import localise_to_utc
from cookiemonster.common.models import Update
from cookiemonster.retriever.manager import PeriodicRetrievalManager, RetrievalManager, MEASURED_RETRIEVAL, \
    MEASURED_RETRIEVAL_STARTED_AT, MEASURED_RETRIEVAL_MOST_RECENT_RETRIEVED, MEASURED_RETRIEVAL_UPDATE_COUNT, \
    MEASURED_RETRIEVAL_DURATION
from cookiemonster.tests.retriever._stubs import StubUpdateMapper

SINCE = localise_to_utc(datetime.min)
TIME_TAKEN_TO_DO_RETRIEVE = 1.0
RETRIEVAL_PERIOD = 0.0001
CURRENT_MONOTONIC_TIME = 0
CURRENT_CLOCK_TIME = datetime(1, 2, 3)


class _BaseRetrievalManagerTest(unittest.TestCase):
    """
    Base class for unit tests on `RetrievalManager` instances.
    """
    def setUp(self):
        self.update_mapper = StubUpdateMapper()
        self.logger = MagicMock()

        self.updates = UpdateCollection([
            Update("a", datetime(year=1999, month=1, day=2), Metadata()),
            Update("b", datetime(year=1998, month=12, day=20), Metadata())
        ])

        def do_query(*args):
            RetrievalManager._get_monotonic_time = MagicMock(
                return_value=RetrievalManager._get_monotonic_time() + RETRIEVAL_PERIOD + TIME_TAKEN_TO_DO_RETRIEVE)
            return self.updates

        self.update_mapper.get_all_since = MagicMock(side_effect=do_query)
        RetrievalManager._get_clock_time = MagicMock(return_value=CURRENT_CLOCK_TIME)

        logging.root.setLevel(level=logging.ERROR)


class TestRetrievalManager(_BaseRetrievalManagerTest):
    """
    Test cases for `RetrievalManager`.
    """
    def setUp(self):
        super().setUp()

        # Create retrieval manager
        self.retrieval_manager = RetrievalManager(self.update_mapper, self.logger)

    def test_run_with_updates(self):
        # Setup
        listener = MagicMock()
        self.retrieval_manager.add_listener(listener)
        self.logger.add = MagicMock()

        # Call SUT method
        self.retrieval_manager.run(SINCE)

        # Assert that retrieves updates from source
        self.update_mapper.get_all_since.assert_called_once_with(SINCE)
        # Assert that updates listeners are called
        listener.assert_called_once_with(self.updates)
        # Assert that retrieval is logged
        self._assert_logged_updated(self.updates)

    def test_run_without_updates(self):
        # Setup
        listener = MagicMock()
        self.retrieval_manager.add_listener(listener)
        self.logger.add = MagicMock()
        self.updates.clear()
        self.retrieval_manager._retrieved_updates_since = CURRENT_MONOTONIC_TIME - (24 * 60 * 60)

        # Call SUT method
        self.retrieval_manager.run(SINCE)

        # Assert that retrieves updates from source
        self.update_mapper.get_all_since.assert_called_once_with(SINCE)
        # Assert that updates listener has not been called given that there are no updates
        listener.assert_not_called()
        # Assert that retrieval is logged
        self._assert_logged_updated(self.updates)

    def _assert_logged_updated(self, updates: UpdateCollection):
        """
        TODO
        :param updates:
        """
        self.assertEqual(self.logger.record.call_count, 1)
        args = self.logger.record.call_args[0]

        self.assertEqual(args[0], MEASURED_RETRIEVAL)
        self.assertEqual(DatetimeISOFormatJSONDecoder().decode(args[1][MEASURED_RETRIEVAL_STARTED_AT]),
                         CURRENT_CLOCK_TIME)
        self.assertGreaterEqual(args[1][MEASURED_RETRIEVAL_DURATION], TIME_TAKEN_TO_DO_RETRIEVE)
        self.assertEqual(args[1][MEASURED_RETRIEVAL_UPDATE_COUNT], len(updates))

        logged_most_recent_retrived = args[1][MEASURED_RETRIEVAL_MOST_RECENT_RETRIEVED]
        if len(updates) > 0:
            self.assertEqual(DatetimeISOFormatJSONDecoder().decode(logged_most_recent_retrived),
                             updates.get_most_recent()[0].timestamp)
        else:
            self.assertIsNone(logged_most_recent_retrived)


class TestPeriodicRetrievalManager(_BaseRetrievalManagerTest):
    """
    Test cases for `PeriodicRetrievalManager`.
    """
    def setUp(self):
        super().setUp()

        # Create retrieval manager
        self.retrieval_manager = PeriodicRetrievalManager(RETRIEVAL_PERIOD, self.update_mapper, self.logger)

    def test_run(self):
        cycles = 10
        listener = MagicMock()

        self.logger.add = MagicMock()
        self.retrieval_manager.add_listener(listener)

        self._setup_to_do_n_cycles(cycles, self.updates)

        self.assertEqual(self.logger.record.call_count, cycles)
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
        # XXX: the logic here only works if the new listener is called after the one added initially
        if updates_each_cycle is None:
            updates_each_cycle = UpdateCollection([])

        stop_lock = Lock()
        stop_lock.acquire()
        wait_for_retrieval_lock = Lock()
        counter = 0

        def on_retrieval(*args, **kwargs):
            nonlocal counter
            counter += 1
            if counter == number_of_cycles:
                stop_lock.release()
            wait_for_retrieval_lock.release()

        def mock_update_getter(*args, **kwargs) -> UpdateCollection:
            wait_for_retrieval_lock.acquire()
            if counter == number_of_cycles - 1:
                # Stop after this retrieval has completed
                self.retrieval_manager.stop()
            return updates_each_cycle

        self.retrieval_manager.add_listener(on_retrieval)
        self.retrieval_manager.update_mapper.get_all_since.side_effect = mock_update_getter

        self.retrieval_manager.start()

        stop_lock.acquire()
        self.retrieval_manager.update_mapper.get_all_since.side_effect = None


    def tearDown(self):
        self.retrieval_manager.stop()


if __name__ == "__main__":
    unittest.main()
