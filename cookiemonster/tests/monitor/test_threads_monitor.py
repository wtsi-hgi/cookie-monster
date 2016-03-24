"""
Authors
-------
* Colin Nolan <cn13@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2016 Genome Research Limited
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
