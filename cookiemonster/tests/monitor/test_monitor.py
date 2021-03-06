"""
Legalese
--------
Copyright (c) 2016 Genome Research Ltd.

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
import unittest
from datetime import timedelta
from threading import Lock
from time import sleep
from unittest.mock import MagicMock

from cookiemonster.logging.logger import PythonLoggingLogger
from cookiemonster.tests.monitor._stubs import StubMonitor


class TestMonitor(unittest.TestCase):
    """
    Tests for `Monitor`.
    """
    def setUp(self):
        self._logger = PythonLoggingLogger()
        self._period = timedelta(microseconds=1)
        self._monitor = StubMonitor(self._logger, self._period)
        self._monitor.do_log_record = MagicMock()

    def tearDown(self):
        self._monitor.stop()

    def test_is_running(self):
        self.assertFalse(self._monitor.is_running())
        self._monitor.start()
        self.assertTrue(self._monitor.is_running())

    def test_start(self):
        lock = Lock()
        lock.acquire()
        self._monitor.do_log_record.side_effect = lock.release
        self._monitor.start()
        self.assertTrue(self._monitor.is_running())
        lock.acquire()

    def test_start_if_started(self):
        self._monitor.start()
        self._monitor.start()
        self.assertTrue(self._monitor.is_running())

    def test_stop(self):
        lock = Lock()
        lock.acquire()
        self._monitor.do_log_record.side_effect = lock.release
        self._monitor.start()
        lock.acquire()
        self._monitor.stop()
        self._monitor.do_log_record.reset_mock()
        sleep(self._period.total_seconds() * 10)
        self.assertEqual(self._monitor.do_log_record.call_count, 0)

    def test_stop_if_not_started(self):
        self._monitor.stop()
        self.assertFalse(self._monitor.is_running())


if __name__ == "__main__":
    unittest.main()
