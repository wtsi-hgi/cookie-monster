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
from unittest.mock import MagicMock

from cookiemonster.monitor.threads_monitor import ThreadsMonitor, MEASURED_NUMBER_OF_THREADS


class TestThreadsMonitor(unittest.TestCase):
    """
    Tests for `ThreadsMonitor`.
    """
    def setUp(self):
        self._logger = MagicMock()
        self._monitor = ThreadsMonitor(self._logger, timedelta(microseconds=1))

    def test_do_log_record(self):
        self._monitor.do_log_record()
        self.assertEqual(self._logger.record.call_count, 1)
        call_args = self._logger.record.call_args[0]
        self.assertEqual(call_args[0], MEASURED_NUMBER_OF_THREADS)
        self.assertGreaterEqual(call_args[1], 1)


if __name__ == "__main__":
    unittest.main()
