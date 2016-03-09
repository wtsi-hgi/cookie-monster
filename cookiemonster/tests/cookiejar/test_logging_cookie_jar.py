import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock

from hgicommon.collections import Metadata

from cookiemonster import Enrichment
from cookiemonster.cookiejar.logging_cookie_jar import LoggingCookieJar, MEASUREMENT_GET_NEXT_FOR_PROCESSING_TIME, \
    MEASUREMENT_ENRICH_COOKIE_TIME, MEASUREMENT_MARK_AS_FAILED_TIME, MEASUREMENT_MARK_AS_COMPLETE_TIME, \
    MEASUREMENT_QUEUE_LENGTH_TIME, MEASUREMENT_MARK_FOR_PROCESSING_TIME


class TestLoggingCookieJar(unittest.TestCase):
    """
    Tests for `LoggingCookieJar`.
    """
    def setUp(self):
        self._logged_cookie_jar = MagicMock()
        self._logger = MagicMock()
        self._cookie_jar = LoggingCookieJar(self._logged_cookie_jar, self._logger)

    def test_enrich_cookie(self):
        source = "my_source"
        enrichment = Enrichment("source", datetime.min, Metadata())
        self._cookie_jar.enrich_cookie(source, enrichment)
        self._logged_cookie_jar.enrich_cookie.assert_called_once_with(source, enrichment)
        self._assert_measured(MEASUREMENT_ENRICH_COOKIE_TIME)

    def test_mark_as_failed(self):
        identifier = "identifier"
        requeue_delay = timedelta(seconds=5)
        self._cookie_jar.mark_as_failed(identifier, requeue_delay)
        self._logged_cookie_jar.mark_as_failed.assert_called_once_with(identifier, requeue_delay)
        self._assert_measured(MEASUREMENT_MARK_AS_FAILED_TIME)

    def test_mark_as_complete(self):
        identifier = "identifier"
        self._cookie_jar.mark_as_complete(identifier)
        self._logged_cookie_jar.mark_as_complete.assert_called_once_with(identifier)
        self._assert_measured(MEASUREMENT_MARK_AS_COMPLETE_TIME)

    def test_mark_for_processing(self):
        identifier = "identifier"
        self._cookie_jar.mark_for_processing(identifier)
        self._logged_cookie_jar.mark_for_processing.assert_called_once_with(identifier)
        self._assert_measured(MEASUREMENT_MARK_FOR_PROCESSING_TIME)

    def test_get_next_for_processing(self):
        self._cookie_jar.get_next_for_processing()
        self._logged_cookie_jar.get_next_for_processing.assert_called_once_with()
        self._assert_measured(MEASUREMENT_GET_NEXT_FOR_PROCESSING_TIME)

    def test_queue_length(self):
        self._cookie_jar.queue_length()
        self._logged_cookie_jar.queue_length.assert_called_once_with()
        self._assert_measured(MEASUREMENT_QUEUE_LENGTH_TIME)

    def _assert_measured(self, measured: str):
        calls = self._logger.record.mock_calls
        self.assertEqual(len(calls), 1)
        args, kwargs = self._logger.record.call_args
        self.assertEqual(args[0], measured)
