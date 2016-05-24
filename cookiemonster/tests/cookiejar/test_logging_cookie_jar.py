"""
Legalese
--------
Copyright (c) 2016 Genome Research Ltd.

Authors:
* Colin Nolan <cn13@sanger.ac.uk>
* Christopher Harrison <ch12@sanger.ac.uk>

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
from datetime import datetime, timedelta
from typing import Dict
from unittest.mock import MagicMock

from hgicommon.collections import Metadata

from cookiemonster.common.models import Enrichment
from cookiemonster.cookiejar import CookieJar
from cookiemonster.cookiejar.in_memory_cookiejar import InMemoryCookieJar
from cookiemonster.cookiejar.logging_cookie_jar import add_cookie_jar_logging, logging_cookie_jar, MEASUREMENT_QUERY_TIME


class TestAddCookieJarLogging(unittest.TestCase):
    """
    Tests for `add_cookie_jar_logging` method.
    """
    def setUp(self):
        self._logger = MagicMock()

        self._cookie_jar = MagicMock()
        self._composite_methods = dict()    # type: Dict[str, MagicMock]
        for method_name in CookieJar.__abstractmethods__:
            method = getattr(self._cookie_jar, method_name)
            method.__name__ = method_name
            self._composite_methods[method_name] = method

        add_cookie_jar_logging(self._cookie_jar, self._logger)

    def test_enrich_cookie(self):
        source = "my_source"
        enrichment = Enrichment("source", datetime.min, Metadata())
        self._cookie_jar.enrich_cookie(source, enrichment)
        self._composite_methods[CookieJar.enrich_cookie.__name__].assert_called_once_with(source, enrichment)
        self._assert_measured(MEASUREMENT_QUERY_TIME[CookieJar.enrich_cookie.__name__])

    def test_mark_as_failed(self):
        identifier = "identifier"
        requeue_delay = timedelta(seconds=5)
        self._cookie_jar.mark_as_failed(identifier, requeue_delay)
        self._composite_methods[CookieJar.mark_as_failed.__name__].assert_called_once_with(identifier, requeue_delay)
        self._assert_measured(MEASUREMENT_QUERY_TIME[CookieJar.mark_as_failed.__name__])

    def test_mark_as_complete(self):
        identifier = "identifier"
        self._cookie_jar.mark_as_complete(identifier)
        self._composite_methods[CookieJar.mark_as_complete.__name__].assert_called_once_with(identifier)
        self._assert_measured(MEASUREMENT_QUERY_TIME[CookieJar.mark_as_complete.__name__])

    def test_mark_for_processing(self):
        identifier = "identifier"
        self._cookie_jar.mark_for_processing(identifier)
        self._composite_methods[CookieJar.mark_for_processing.__name__].assert_called_once_with(identifier)
        self._assert_measured(MEASUREMENT_QUERY_TIME[CookieJar.mark_for_processing.__name__])

    def test_get_next_for_processing(self):
        self._cookie_jar.get_next_for_processing()
        self._composite_methods[CookieJar.get_next_for_processing.__name__].assert_called_once_with()
        self._assert_measured(MEASUREMENT_QUERY_TIME[CookieJar.get_next_for_processing.__name__])

    def test_queue_length(self):
        self._cookie_jar.queue_length()
        self._composite_methods[CookieJar.queue_length.__name__].assert_called_once_with()
        self._assert_measured(MEASUREMENT_QUERY_TIME[CookieJar.queue_length.__name__])

    def test_call_to_non_adapted_cookie_jar_method(self):
        self._cookie_jar = InMemoryCookieJar()
        self._cookie_jar._composite_cookie_jar = self._cookie_jar

        listener = lambda: None
        self._cookie_jar.add_listener(listener)
        assert self._cookie_jar.get_listeners() == [listener]
        self.assertEqual(self._cookie_jar.get_listeners, self._cookie_jar.get_listeners)
        self.assertEqual(self._cookie_jar.get_listeners(), [listener])

    def test_call_to_non_cookie_jar_method(self):
        self._cookie_jar.some_method = MagicMock()
        self._cookie_jar.some_method()
        self._cookie_jar.some_method.assert_called_once_with()

    def test_call_to_non_cookie_jar_property(self):
        self._cookie_jar.some_property = 1
        self.assertEqual(self._cookie_jar.some_property, 1)

    def _assert_measured(self, measured: str):
        calls = self._logger.record.mock_calls
        self.assertEqual(len(calls), 1)
        args, kwargs = self._logger.record.call_args
        self.assertEqual(args[0], measured)


class TestLoggingCookieJar(unittest.TestCase):
    """
    Tests for `logging_cookie_jar` method.
    """
    def setUp(self):
        self._logger = MagicMock()

    def test_create_logging_cookie_jar(self):
        InMemoryLoggingCookieJar = logging_cookie_jar(self._logger)(InMemoryCookieJar)
        cookie_jar = InMemoryLoggingCookieJar()
        queue_length = cookie_jar.queue_length()
        self.assertEqual(queue_length, 0)
        self.assertEqual(self._logger.record.call_count, 1)

    def test_use_as_decorator(self):
        @logging_cookie_jar(self._logger)
        class InMemoryLoggingCookieJar(InMemoryCookieJar):
            pass

        cookie_jar = InMemoryLoggingCookieJar()
        queue_length = cookie_jar.queue_length()
        self.assertEqual(queue_length, 0)
        self.assertEqual(self._logger.record.call_count, 1)
