"""
Cookie Jar Implementation Tests
===============================
High-level integration and unit tests of `CookieJar` implementations.
Specifically: `BiscuitTin`, the buffered, CouchDB-backed implementation
meant for production; `RateLimitedBiscuitTin`, a rate-limited version
of `BiscuitTin`; and `InMemoryCookieJar`, an in-memory implementation
used in development and testing.

The following sequences are tested:

* Get Next

* Enrich -> Get Next

* Enrich -> Enrich Again -> Get Next

* Enrich -> Get Next -> Mark Complete

* Enrich 1 -> Enrich 2 -> Get Next (X) -> Get Next (Y) -> Mark X
  Complete -> Mark Y Complete

* Enrich 1 -> Enrich 2 -> Get Next (X) -> Mark X Complete -> Get Next
  (Y) -> Mark Y Complete

* Enrich -> Get Next -> Mark Failed Immediate -> Get Next

* Enrich -> Get Next -> Mark Failed 3s Delay -> Queue Empty Until Delay

* Enrich -> Get Next -> Enrich same -> Mark Complete -> Get Next

* Enrich -> Get Next -> Mark Complete -> Mark Reprocess -> Get Next

The following sequences are specific to `BiscuitTin` and derivatives:

* Enrich -> Reconnect (i.e., simulate failure) -> Get Next

* Enrich -> Get Next -> Reconnect -> Get Next

* Enrich -> Fetch by Identifier

* Enrich -> Delete by Identifier

* Enrich -> Get Next -> Delete -> Mark Complete

Legalese
--------
Copyright (c) 2015, 2016 Genome Research Ltd.

Authors:
* Christopher Harrison <ch12@sanger.ac.uk>
* Colin Nolan <cn13@sanger.ac.uk>

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
from unittest.mock import MagicMock
from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta, timezone
from threading import Timer
from typing import Any, Callable
from numbers import Real

from cookiemonster.cookiejar import CookieJar, BiscuitTin, RateLimitedBiscuitTin
from cookiemonster.cookiejar.in_memory_cookiejar import InMemoryCookieJar
from cookiemonster.common.models import Enrichment, Cookie
from cookiemonster.tests._utils.docker_couchdb import CouchDBContainer

from hgicommon.collections import Metadata

# We need this for mocking
import cookiemonster.cookiejar.biscuit_tin as _biscuit_tin


class TestCookieJar(unittest.TestCase, metaclass=ABCMeta):
    """
    Tests for implementations of `CookieJar`
    """
    def setUp(self):
        """
        Provide sample inputs with which to test
        """
        self.eg_identifiers = [
            '/foo',
            '/bar/baz'
        ]
        self.eg_metadata = [
            Metadata({'xyzzy': 123}),
            Metadata({'quux': 'snuffleupagus'})
        ]
        self.eg_enrichments = [
            Enrichment('random', datetime(1981, 9, 25, 5, 55, tzinfo=timezone.utc), self.eg_metadata[0]),
            Enrichment('irods', datetime.now(tz=timezone.utc).replace(microsecond=0), self.eg_metadata[1])
        ]
        self.eg_listener = MagicMock()

        self.jar = self._create_cookie_jar()
        self.jar.add_listener(self.eg_listener)

        # Change time zone to Testing Standard Time ;)
        self._change_time(self.jar, 123456)

    @abstractmethod
    def _create_cookie_jar(self) -> CookieJar:
        """
        Creates a cookie jar as the SUT.
        :return: cookie jar that is to be tested
        """

    @abstractmethod
    def _change_time(self, cookie_jar: CookieJar, change_time_to: int):
        """
        Changes the time in the given cookie jar.
        :param cookie_jar: cookie jar to change the time in
        :param change_time_to: the time to change to
        """

    @abstractmethod
    def _get_scheduled_fn(self) -> Callable[..., Any]:
        """
        @return  The scheduling function
        """

    @abstractmethod
    def _test_scheduling(self, expected_timeout:Real, expected_call:Callable[..., Any]):
        """
        Test the scheduling
        @param  expected_timeout  Time until invocation
        @param  expected_call     Callable to invoke
        """

    def test01_empty_queue(self):
        """
        CookieJar Sequence: Get Next
        """
        self.assertEqual(self.jar.queue_length(), 0)
        self.assertIsNone(self.jar.get_next_for_processing())
        self.eg_listener.assert_not_called()

    def test02_single_enrichment(self):
        """
        CookieJar Sequence: Enrich -> Get Next
        """
        self.jar.enrich_cookie(self.eg_identifiers[0], self.eg_enrichments[0])
        self.assertEqual(self.jar.queue_length(), 1)

        to_process = self.jar.get_next_for_processing()

        self.assertEqual(self.jar.queue_length(), 0)
        self.assertIsInstance(to_process, Cookie)
        self.assertEqual(to_process.identifier, self.eg_identifiers[0])
        self.assertEqual(len(to_process.enrichments), 1)
        self.assertEqual(to_process.enrichments[0], self.eg_enrichments[0])
        self.assertEqual(self.eg_listener.call_count, 1)

    def test_single_enrichment_with_no_processing(self):
        """
        CookieJar Sequence: Enrich -> Get Next
        """
        self.jar.enrich_cookie(self.eg_identifiers[0], self.eg_enrichments[0], False)
        self.assertEqual(self.jar.queue_length(), 0)

    def test03_multiple_enrichment(self):
        """
        CookieJar Sequence: Enrich -> Enrich Again -> Get Next
        """
        self.jar.enrich_cookie(self.eg_identifiers[0], self.eg_enrichments[0])
        self.assertEqual(self.jar.queue_length(), 1)

        self.jar.enrich_cookie(self.eg_identifiers[0], self.eg_enrichments[1])
        self.assertEqual(self.jar.queue_length(), 1)

        to_process = self.jar.get_next_for_processing()

        self.assertEqual(self.jar.queue_length(), 0)
        self.assertIsInstance(to_process, Cookie)
        self.assertEqual(to_process.identifier, self.eg_identifiers[0])
        self.assertEqual(len(to_process.enrichments), 2)
        self.assertEqual(to_process.enrichments[0], self.eg_enrichments[0])
        self.assertEqual(to_process.enrichments[1], self.eg_enrichments[1])
        self.assertEqual(self.eg_listener.call_count, 2)

    def test04_enrich_and_complete(self):
        """
        CookieJar Sequence: Enrich -> Get Next -> Mark Complete
        """
        self.jar.enrich_cookie(self.eg_identifiers[0], self.eg_enrichments[0])
        to_process = self.jar.get_next_for_processing()
        self.jar.mark_as_complete(to_process.identifier)
        self.assertEqual(self.jar.queue_length(), 0)
        self.assertEqual(self.eg_listener.call_count, 1)

    def test05_process_multiple(self):
        """
        CookieJar Sequence: Enrich 1 -> Enrich 2 -> Get Next (X) -> Get Next (Y) -> Mark X Complete -> Mark Y Complete
        """
        self.jar.enrich_cookie(self.eg_identifiers[0], self.eg_enrichments[0])

        # Fast forward one second
        self._change_time(self.jar, 123457)

        self.jar.enrich_cookie(self.eg_identifiers[1], self.eg_enrichments[1])
        self.assertEqual(self.jar.queue_length(), 2)

        first = self.jar.get_next_for_processing()

        self.assertEqual(self.jar.queue_length(), 1)
        self.assertIsInstance(first, Cookie)
        self.assertEqual(first.identifier, self.eg_identifiers[0])
        self.assertEqual(len(first.enrichments), 1)
        self.assertEqual(first.enrichments[0], self.eg_enrichments[0])

        second = self.jar.get_next_for_processing()

        self.assertEqual(self.jar.queue_length(), 0)
        self.assertIsInstance(second, Cookie)
        self.assertEqual(second.identifier, self.eg_identifiers[1])
        self.assertEqual(len(second.enrichments), 1)
        self.assertEqual(second.enrichments[0], self.eg_enrichments[1])

        self.jar.mark_as_complete(first.identifier)
        self.assertEqual(self.jar.queue_length(), 0)

        self.jar.mark_as_complete(second.identifier)
        self.assertEqual(self.jar.queue_length(), 0)
        self.assertEqual(self.eg_listener.call_count, 2)

    def test06_process_multiple_intertwined(self):
        """
        CookieJar Sequence: Enrich 1 -> Enrich 2 -> Get Next (X) -> Mark X Complete -> Get Next (Y) -> Mark Y Complete
        """
        self.jar.enrich_cookie(self.eg_identifiers[0], self.eg_enrichments[0])
        self.jar.enrich_cookie(self.eg_identifiers[1], self.eg_enrichments[1])
        self.assertEqual(self.jar.queue_length(), 2)

        first = self.jar.get_next_for_processing()
        self.jar.mark_as_complete(first.identifier)
        self.assertEqual(self.jar.queue_length(), 1)

        second = self.jar.get_next_for_processing()
        self.jar.mark_as_complete(second.identifier)
        self.assertEqual(self.jar.queue_length(), 0)
        self.assertEqual(self.eg_listener.call_count, 2)

    def test07_fail_immediate(self):
        """
        CookieJar Sequence: Enrich -> Get Next -> Mark Failed Immediate -> Get Next
        """
        self.jar.enrich_cookie(self.eg_identifiers[0], self.eg_enrichments[0])
        before = self.jar.get_next_for_processing()
        self.jar.mark_as_failed(before.identifier)

        # Test that the queue length broadcast has been scheduled and
        # the queue length has changed appropriately
        self._test_scheduling(0, self._get_scheduled_fn())
        self.assertEqual(self.jar.queue_length(), 1)

        after = self.jar.get_next_for_processing()
        self.assertEqual(self.jar.queue_length(), 0)
        self.assertEqual(before, after)

    def test08_fail_delayed(self):
        """
        CookieJar Sequence: Enrich -> Get Next -> Mark Failed 3s Delay -> Queue Empty Until Delay
        """
        self.jar.enrich_cookie(self.eg_identifiers[0], self.eg_enrichments[0])
        to_process = self.jar.get_next_for_processing()

        self.jar.mark_as_failed(to_process.identifier, timedelta(seconds=3))
        self.assertEqual(self.jar.queue_length(), 0)

        # Test that the queue length broadcast has been scheduled
        self._test_scheduling(3, self._get_scheduled_fn())

        # +1 second
        self._change_time(self.jar, 123457)
        self.assertEqual(self.jar.queue_length(), 0)

        # +2 seconds
        self._change_time(self.jar, 123458)
        self.assertEqual(self.jar.queue_length(), 0)

        # +3 seconds: Queue length should change
        self._change_time(self.jar, 123459)
        self.assertEqual(self.jar.queue_length(), 1)

        # NOTE This *doesn't* test that the listener is called exactly
        # when the new data "come online", as this is a passive process
        # of the database view. It just shows that the listener will be
        # called after the specified time and that, at that time, the
        # queue length is correct... Due to the inexactness of the Timer
        # there could be possible synchronisation issues.

    def test09_out_of_order_enrichment(self):
        """
        CookieJar Sequence: Enrich -> Get Next -> Enrich same -> Mark Complete -> Get Next
        """
        self.jar.enrich_cookie(self.eg_identifiers[0], self.eg_enrichments[0])
        to_process = self.jar.get_next_for_processing()
        self.jar.enrich_cookie(self.eg_identifiers[0], self.eg_enrichments[1])
        self.assertEqual(self.jar.queue_length(), 0)

        self.jar.mark_as_complete(to_process.identifier)
        self.assertEqual(self.jar.queue_length(), 1)

        to_process = self.jar.get_next_for_processing()

        self.assertEqual(self.jar.queue_length(), 0)
        self.assertIsInstance(to_process, Cookie)
        self.assertEqual(to_process.identifier, self.eg_identifiers[0])
        self.assertEqual(len(to_process.enrichments), 2)
        self.assertEqual(to_process.enrichments[0], self.eg_enrichments[0])
        self.assertEqual(to_process.enrichments[1], self.eg_enrichments[1])
        self.assertEqual(self.eg_listener.call_count, 2)

    def test10_reprocess(self):
        """
        CookieJar Sequence: Enrich -> Get Next -> Mark Complete -> Mark Reprocess -> Get Next
        """
        self.jar.enrich_cookie(self.eg_identifiers[0], self.eg_enrichments[0])
        before = self.jar.get_next_for_processing()
        self.jar.mark_as_complete(before.identifier)

        self.jar.mark_for_processing(before.identifier)
        self.assertEqual(self.jar.queue_length(), 1)

        after = self.jar.get_next_for_processing()
        self.assertEqual(self.jar.queue_length(), 0)
        self.assertEqual(before, after)
        self.assertEqual(self.eg_listener.call_count, 2)

    def test11_fetch_by_id(self):
        """
        CookieJar Sequence: Enrich -> Fetch by Identifier
        """
        self.jar.enrich_cookie(self.eg_identifiers[0], self.eg_enrichments[0])

        cookie = self.jar.fetch_cookie(self.eg_identifiers[0])
        self.assertIsInstance(cookie, Cookie)
        self.assertEqual(cookie.identifier, self.eg_identifiers[0])
        self.assertEqual(len(cookie.enrichments), 1)
        self.assertEqual(cookie.enrichments[0], self.eg_enrichments[0])

        no_cookie = self.jar.fetch_cookie('this does not exist')
        self.assertIsNone(no_cookie)

    def test12_delete_by_id(self):
        """
        CookieJar Sequence: Enrich -> Delete by Identifer
        """
        self.jar.enrich_cookie(self.eg_identifiers[0], self.eg_enrichments[0])

        cookie = self.jar.fetch_cookie(self.eg_identifiers[0])
        self.assertIsInstance(cookie, Cookie)

        self.jar.delete_cookie(self.eg_identifiers[0])

        deleted_cookie = self.jar.fetch_cookie(self.eg_identifiers[0])
        self.assertIsNone(deleted_cookie)

    def test13_delete_while_processing(self):
        """
        CookieJar Sequence: Enrich -> Get Next -> Delete -> Mark Complete
        """
        ident = self.eg_identifiers[0]

        self.jar.enrich_cookie(ident, self.eg_enrichments[0])
        to_process = self.jar.get_next_for_processing()
        self.assertEqual(to_process.identifier, ident)
        self.jar.delete_cookie(ident)

        # A cookie that is deleted while processing will still exist,
        # except for its enrichments (which are removed)
        to_be_deleted = self.jar.fetch_cookie(ident)
        self.assertIsInstance(to_be_deleted, Cookie)
        self.assertEqual(to_be_deleted.identifier, ident)
        self.assertEqual(len(to_be_deleted.enrichments), 0)

        self.jar.mark_as_complete(ident)

        # Once completed, the cookie should be completely removed
        deleted = self.jar.fetch_cookie(ident)
        self.assertIsNone(deleted)


class TestBiscuitTin(TestCookieJar):
    """
    High-level integration and logic tests of the CookieJar-CouchDB
    implementation (`BiscuitTin`). We assume that if the higher-level tests
    pass and are suitably comprehensive, then the underlying levels of
    abstraction are probably fine™
    """
    def setUp(self):
        self.couchdb_container = CouchDBContainer()
        self.HOST = self.couchdb_container.couchdb_fqdn
        self.DB   = 'cookiejar-test'

        _biscuit_tin.Timer = MagicMock()

        super().setUp()

    def tearDown(self):
        self.couchdb_container.tear_down()
        _biscuit_tin.Timer.reset_mock()

    def _create_cookie_jar(self) -> BiscuitTin:
        # TODO? We don't test the buffering (only the trivial case of a
        # single document, zero-latency buffer)
        return BiscuitTin(self.HOST, self.DB, 1, timedelta(0))

    def _get_scheduled_fn(self) -> Callable[..., Any]:
        return self.jar._broadcast

    def _test_scheduling(self, expected_timeout:Real, expected_call:Callable[..., Any]):
        _biscuit_tin.Timer.assert_called_with(expected_timeout, expected_call)

    def _change_time(self, cookie_jar: CookieJar, change_time_to: int):
        _biscuit_tin._now = MagicMock(return_value=change_time_to)

    def test14_connection_failure(self):
        """
        CookieJar Sequence: Enrich -> Reconnect -> Get Next
        """
        self.jar.enrich_cookie(self.eg_identifiers[0], self.eg_enrichments[0])
        new_jar = self._create_cookie_jar()

        self.assertEqual(new_jar.queue_length(), 1)

        to_process = new_jar.get_next_for_processing()

        self.assertEqual(new_jar.queue_length(), 0)
        self.assertIsInstance(to_process, Cookie)
        self.assertEqual(to_process.identifier, self.eg_identifiers[0])
        self.assertEqual(len(to_process.enrichments), 1)
        self.assertEqual(to_process.enrichments[0], self.eg_enrichments[0])

    def test15_connection_failure_while_processing(self):
        """
        CookieJar Sequence: Enrich -> Get Next -> Reconnect -> Get Next
        """
        self.jar.enrich_cookie(self.eg_identifiers[0], self.eg_enrichments[0])
        before = self.jar.get_next_for_processing()

        new_jar = self._create_cookie_jar()
        self.assertEqual(new_jar.queue_length(), 1)

        after = new_jar.get_next_for_processing()
        self.assertEqual(before, after)


class TestRateLimitedBiscuitTin(TestBiscuitTin):
    """
    Tests for `RateLimitedBiscuitTin`.
    """
    def _create_cookie_jar(self) -> RateLimitedBiscuitTin:
        return RateLimitedBiscuitTin(10, self.HOST, self.DB, 1, timedelta(0))


class TestInMemoryCookieJar(TestCookieJar):
    """
    Tests for `InMemoryCookieJar`.
    """
    def _create_cookie_jar(self) -> CookieJar:
        return InMemoryCookieJar()

    def _get_scheduled_fn(self) -> Callable[..., Any]:
        return MagicMock()

    def _test_scheduling(self, expected_timeout:Real, expected_call:Callable[..., Any]):
        pass

    def _change_time(self, cookie_jar: InMemoryCookieJar, change_time_to: int):
        cookie_jar._get_time = MagicMock(return_value=change_time_to)

        for end_time in cookie_jar._timers.keys():
            if end_time <= change_time_to:
                while len(cookie_jar._timers[end_time]) > 0:
                    timer = cookie_jar._timers[end_time][0]    # type: Timer
                    timer.interval = 0
                    timer.run()


# Trick required to stop Python's unittest from running the abstract base class as a test
del TestCookieJar


if __name__ == '__main__':
    unittest.main()
