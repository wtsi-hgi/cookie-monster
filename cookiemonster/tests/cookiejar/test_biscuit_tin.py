'''
Cookie Jar Implementation Test
==============================
High-level integration and logic tests of the CookieJar-CouchDB
implementation (`BiscuitTin`). We assume that if the higher-level tests
pass and are suitably comprehensive, then the underlying levels of
abstraction are probably fine™.

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

* Enrich -> Reconnect (i.e., simulate failure) -> Get Next

* Enrich -> Get Next -> Reconnect -> Get Next

TODO Others?

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015, 2016 Genome Research Limited
'''
from datetime import datetime, timedelta
import unittest
from unittest.mock import MagicMock

from cookiemonster.cookiejar import BiscuitTin
from cookiemonster.tests._utils.docker_couchdb import CouchDBContainer
from cookiemonster.common.models import Enrichment, Cookie

from hgicommon.collections import Metadata

# We need these for mocking
import cookiemonster.cookiejar._dbi as dbi
import cookiemonster.cookiejar.biscuit_tin as biscuit_tin


def _change_time(time):
    ''' Mock the timing so we can control it '''
    dbi._now = MagicMock(return_value=time)

biscuit_tin.Timer = MagicMock()


class TestCookieJar(unittest.TestCase):
    def setUp(self):
        '''
        Build, if necessary, and start a Dockerised CouchDB instance and
        connect. Plus, provide sample inputs with which to test.
        '''
        self.couchdb_container = CouchDBContainer()

        self.HOST = self.couchdb_container.couchdb_fqdn
        self.DB   = 'cookiejar-test'

        # TODO? We don't test the buffering (only the trivial case of a
        # single document, zero-latency buffer)
        self.jar = BiscuitTin(self.HOST, self.DB, 1, timedelta(0))

        self.eg_paths = [
            '/foo',
            '/bar/baz'
        ]
        self.eg_metadata = [
            Metadata({'xyzzy': 123}),
            Metadata({'quux': 'snuffleupagus'})
        ]
        self.eg_enrichments = [
            Enrichment('random', datetime(1981, 9, 25, 5, 55), self.eg_metadata[0]),
            Enrichment("irods", datetime.now().replace(microsecond=0), self.eg_metadata[1])
        ]
        self.eg_listener = MagicMock()

        self.jar.add_listener(self.eg_listener)

        # Change time zone to Testing Standard Time ;)
        _change_time(123456)

    def tearDown(self):
        ''' Tear down CouchDB container '''
        self.couchdb_container.tear_down()
        biscuit_tin.Timer.reset_mock()

    def test01_empty_queue(self):
        '''
        CookieJar Sequence: Get Next
        '''
        self.assertEqual(self.jar.queue_length(), 0)
        self.assertIsNone(self.jar.get_next_for_processing())
        self.eg_listener.assert_not_called()

    def test02_single_enrichment(self):
        '''
        CookieJar Sequence: Enrich -> Get Next
        '''
        self.jar.enrich_cookie(self.eg_paths[0], self.eg_enrichments[0])
        self.assertEqual(self.jar.queue_length(), 1)

        to_process = self.jar.get_next_for_processing()

        self.assertEqual(self.jar.queue_length(), 0)
        self.assertIsInstance(to_process, Cookie)
        self.assertEqual(to_process.path, self.eg_paths[0])
        self.assertEqual(len(to_process.enrichments), 1)
        self.assertEqual(to_process.enrichments[0], self.eg_enrichments[0])
        self.assertEqual(self.eg_listener.call_count, 1)

    def test03_multiple_enrichment(self):
        '''
        CookieJar Sequence: Enrich -> Enrich Again -> Get Next
        '''
        self.jar.enrich_cookie(self.eg_paths[0], self.eg_enrichments[0])
        self.assertEqual(self.jar.queue_length(), 1)

        self.jar.enrich_cookie(self.eg_paths[0], self.eg_enrichments[1])
        self.assertEqual(self.jar.queue_length(), 1)

        to_process = self.jar.get_next_for_processing()

        self.assertEqual(self.jar.queue_length(), 0)
        self.assertIsInstance(to_process, Cookie)
        self.assertEqual(to_process.path, self.eg_paths[0])
        self.assertEqual(len(to_process.enrichments), 2)
        self.assertEqual(to_process.enrichments[0], self.eg_enrichments[0])
        self.assertEqual(to_process.enrichments[1], self.eg_enrichments[1])
        self.assertEqual(self.eg_listener.call_count, 2)

    def test04_enrich_and_complete(self):
        '''
        CookieJar Sequence: Enrich -> Get Next -> Mark Complete
        '''
        self.jar.enrich_cookie(self.eg_paths[0], self.eg_enrichments[0])
        to_process = self.jar.get_next_for_processing()
        self.jar.mark_as_complete(to_process.path)
        self.assertEqual(self.jar.queue_length(), 0)
        self.assertEqual(self.eg_listener.call_count, 1)

    def test05_process_multiple(self):
        '''
        CookieJar Sequence: Enrich 1 -> Enrich 2 -> Get Next (X) -> Get Next (Y) -> Mark X Complete -> Mark Y Complete
        '''
        self.jar.enrich_cookie(self.eg_paths[0], self.eg_enrichments[0])

        # Fast forward one second
        _change_time(123457)

        self.jar.enrich_cookie(self.eg_paths[1], self.eg_enrichments[1])
        self.assertEqual(self.jar.queue_length(), 2)

        first = self.jar.get_next_for_processing()

        self.assertEqual(self.jar.queue_length(), 1)
        self.assertIsInstance(first, Cookie)
        self.assertEqual(first.path, self.eg_paths[0])
        self.assertEqual(len(first.enrichments), 1)
        self.assertEqual(first.enrichments[0], self.eg_enrichments[0])

        second = self.jar.get_next_for_processing()

        self.assertEqual(self.jar.queue_length(), 0)
        self.assertIsInstance(second, Cookie)
        self.assertEqual(second.path, self.eg_paths[1])
        self.assertEqual(len(second.enrichments), 1)
        self.assertEqual(second.enrichments[0], self.eg_enrichments[1])

        self.jar.mark_as_complete(first.path)
        self.assertEqual(self.jar.queue_length(), 0)

        self.jar.mark_as_complete(second.path)
        self.assertEqual(self.jar.queue_length(), 0)
        self.assertEqual(self.eg_listener.call_count, 2)

    def test06_process_multiple_intertwined(self):
        '''
        CookieJar Sequence: Enrich 1 -> Enrich 2 -> Get Next (X) -> Mark X Complete -> Get Next (Y) -> Mark Y Complete
        '''
        self.jar.enrich_cookie(self.eg_paths[0], self.eg_enrichments[0])
        self.jar.enrich_cookie(self.eg_paths[1], self.eg_enrichments[1])
        self.assertEqual(self.jar.queue_length(), 2)

        first = self.jar.get_next_for_processing()
        self.jar.mark_as_complete(first.path)
        self.assertEqual(self.jar.queue_length(), 1)

        second = self.jar.get_next_for_processing()
        self.jar.mark_as_complete(second.path)
        self.assertEqual(self.jar.queue_length(), 0)
        self.assertEqual(self.eg_listener.call_count, 2)

    def test07_fail_immediate(self):
        '''
        CookieJar Sequence: Enrich -> Get Next -> Mark Failed Immediate -> Get Next
        '''
        self.jar.enrich_cookie(self.eg_paths[0], self.eg_enrichments[0])
        before = self.jar.get_next_for_processing()
        self.jar.mark_as_failed(before.path)

        # Test that the queue length broadcast has been scheduled and
        # the queue length has changed appropriately
        biscuit_tin.Timer.assert_called_with(0, self.jar._broadcast_length)
        self.assertEqual(self.jar.queue_length(), 1)

        after = self.jar.get_next_for_processing()
        self.assertEqual(self.jar.queue_length(), 0)
        self.assertEqual(before, after)

    def test08_fail_delayed(self):
        '''
        CookieJar Sequence: Enrich -> Get Next -> Mark Failed 3s Delay -> Queue Empty Until Delay
        '''
        self.jar.enrich_cookie(self.eg_paths[0], self.eg_enrichments[0])
        to_process = self.jar.get_next_for_processing()

        self.jar.mark_as_failed(to_process.path, timedelta(seconds=3))
        self.assertEqual(self.jar.queue_length(), 0)

        # Test that the queue length broadcast has been scheduled
        biscuit_tin.Timer.assert_called_with(3, self.jar._broadcast_length)

        # +1 second
        _change_time(123457)
        self.assertEqual(self.jar.queue_length(), 0)

        # +2 seconds
        _change_time(123458)
        self.assertEqual(self.jar.queue_length(), 0)

        # +3 seconds: Queue length should change
        _change_time(123459)
        self.assertEqual(self.jar.queue_length(), 1)

        # FIXME? This *doesn't* test that the listener is called at the
        # appropriate time with the appropriate arguments; it just shows
        # that the listener will be called after the appropriate time
        # and that, at that time, the queue length (i.e., its argument)
        # is correct. There could be possible synchronisation issues due
        # to the inexactness of the Timer.

    def test09_out_of_order_enrichment(self):
        '''
        CookieJar Sequence: Enrich -> Get Next -> Enrich same -> Mark Complete -> Get Next
        '''
        self.jar.enrich_cookie(self.eg_paths[0], self.eg_enrichments[0])
        to_process = self.jar.get_next_for_processing()
        self.jar.enrich_cookie(self.eg_paths[0], self.eg_enrichments[1])
        self.assertEqual(self.jar.queue_length(), 0)

        self.jar.mark_as_complete(to_process.path)
        self.assertEqual(self.jar.queue_length(), 1)

        to_process = self.jar.get_next_for_processing()

        self.assertEqual(self.jar.queue_length(), 0)
        self.assertIsInstance(to_process, Cookie)
        self.assertEqual(to_process.path, self.eg_paths[0])
        self.assertEqual(len(to_process.enrichments), 2)
        self.assertEqual(to_process.enrichments[0], self.eg_enrichments[0])
        self.assertEqual(to_process.enrichments[1], self.eg_enrichments[1])
        self.assertEqual(self.eg_listener.call_count, 2)

    def test10_reprocess(self):
        '''
        CookieJar Sequence: Enrich -> Get Next -> Mark Complete -> Mark Reprocess -> Get Next
        '''
        self.jar.enrich_cookie(self.eg_paths[0], self.eg_enrichments[0])
        before = self.jar.get_next_for_processing()
        self.jar.mark_as_complete(before.path)

        self.jar.mark_for_processing(before.path)
        self.assertEqual(self.jar.queue_length(), 1)

        after = self.jar.get_next_for_processing()
        self.assertEqual(self.jar.queue_length(), 0)
        self.assertEqual(before, after)
        self.assertEqual(self.eg_listener.call_count, 2)

    def test11_connection_failure(self):
        '''
        CookieJar Sequence: Enrich -> Reconnect -> Get Next
        '''
        self.jar.enrich_cookie(self.eg_paths[0], self.eg_enrichments[0])
        new_jar = BiscuitTin(self.HOST, self.DB, 1, timedelta(0))

        self.assertEqual(new_jar.queue_length(), 1)

        to_process = new_jar.get_next_for_processing()

        self.assertEqual(new_jar.queue_length(), 0)
        self.assertIsInstance(to_process, Cookie)
        self.assertEqual(to_process.path, self.eg_paths[0])
        self.assertEqual(len(to_process.enrichments), 1)
        self.assertEqual(to_process.enrichments[0], self.eg_enrichments[0])

    def test12_connection_failure_while_processing(self):
        '''
        CookieJar Sequence: Enrich -> Get Next -> Reconnect -> Get Next
        '''
        self.jar.enrich_cookie(self.eg_paths[0], self.eg_enrichments[0])
        before = self.jar.get_next_for_processing()

        new_jar = BiscuitTin(self.HOST, self.DB, 1, timedelta(0))
        self.assertEqual(new_jar.queue_length(), 1)

        after = new_jar.get_next_for_processing()
        self.assertEqual(before, after)


if __name__ == '__main__':
    unittest.main()
