import unittest
from datetime import timedelta, datetime
from unittest.mock import MagicMock

from hgicommon.collections import Metadata

from cookiemonster import Enrichment
from cookiemonster.cookiejar.in_memory_cookiejar import InMemoryCookieJar
from cookiemonster.monitor.cookiejar_monitor import CookieJarMonitor, MEASURED_COOKIE_JAR_TO_PROCESS, \
    MEASURED_COOKIE_JAR_STATUS


class TestThreadsMonitor(unittest.TestCase):
    """
    Tests for `ThreadsMonitor`.
    """
    def setUp(self):
        self._logger = MagicMock()
        self._cookie_jar = InMemoryCookieJar()
        self._monitor = CookieJarMonitor(self._logger, timedelta(microseconds=1), self._cookie_jar)

    def test_do_log_record(self):
        self._cookie_jar.enrich_cookie("test", Enrichment("test", datetime(1, 2, 3), Metadata()))
        self._monitor.do_log_record()
        self.assertEqual(self._logger.record.call_count, 1)
        call_args = self._logger.record.call_args[0]
        self.assertEqual(call_args[0], MEASURED_COOKIE_JAR_STATUS)
        self.assertEqual(call_args[1], {MEASURED_COOKIE_JAR_TO_PROCESS: 1})


if __name__ == "__main__":
    unittest.main()
