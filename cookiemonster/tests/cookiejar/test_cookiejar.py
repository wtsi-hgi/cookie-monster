import unittest
from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta
from unittest.mock import MagicMock

from hgicommon.collections import Metadata

from cookiemonster.cookiejar import _dbi
from cookiemonster.common.models import Enrichment, Cookie
from cookiemonster.cookiejar import CookieJar, BiscuitTin
from cookiemonster.cookiejar.in_memory_cookiejar import InMemoryCookieJar
from cookiemonster.tests._utils.docker_couchdb import CouchDBContainer


class TestCookieJar(unittest.TestCase, metaclass=ABCMeta):
    """
    Tests for implementations of `CookieJar`.
    """
    def setUp(self):
        '''
        Build, if necessary, and start a Dockerised CouchDB instance and
        connect. Plus, provide sample inputs with which to test.
        '''
        self.eg_paths = ['/foo',
                         '/bar/baz']
        self.eg_metadata = [Metadata({'xyzzy': 123}),
                            Metadata({'quux': 'snuffleupagus'})]
        self.eg_enrichments = [Enrichment('random', datetime(1981, 9, 25, 5, 55), self.eg_metadata[0]),
                               Enrichment("irods", datetime(2015, 12, 9, 9), self.eg_metadata[1])]
        self.eg_listener = MagicMock()

        self.jar = self._create_cookie_jar()
        self.jar.add_listener(self.eg_listener)

        # Change time zone to Testing Standard Time ;)
        self._change_time(123456)

    @abstractmethod
    def _create_cookie_jar(self) -> CookieJar:
        """
        Creates a cookie jar as the SUT.
        :return: cookie jar that is to be tested
        """

    @abstractmethod
    def _change_time(self, change_time_to: int):
        """
        Changes the time considered in tests to that given.
        :param change_time_to: the time to change to
        """

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
        self.assertEquals(self.eg_listener.call_count, 1)

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
        self.assertEquals(self.eg_listener.call_count, 2)

    def test04_enrich_and_complete(self):
        '''
        CookieJar Sequence: Enrich -> Get Next -> Mark Complete
        '''
        self.jar.enrich_cookie(self.eg_paths[0], self.eg_enrichments[0])
        to_process = self.jar.get_next_for_processing()
        self.jar.mark_as_complete(to_process.path)
        self.assertEqual(self.jar.queue_length(), 0)
        self.assertEquals(self.eg_listener.call_count, 1)

    def test05_process_multiple(self):
        '''
        CookieJar Sequence: Enrich 1 -> Enrich 2 -> Get Next (X) -> Get Next (Y) -> Mark X Complete -> Mark Y Complete
        '''
        self.jar.enrich_cookie(self.eg_paths[0], self.eg_enrichments[0])

        # Fast forward one second
        self._change_time(123457)

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
        self.assertEquals(self.eg_listener.call_count, 2)

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
        self.assertEquals(self.eg_listener.call_count, 2)

    def test07_fail_immediate(self):
        '''
        CookieJar Sequence: Enrich -> Get Next -> Mark Failed Immediate -> Get Next
        '''
        self.jar.enrich_cookie(self.eg_paths[0], self.eg_enrichments[0])
        before = self.jar.get_next_for_processing()
        self.jar.mark_as_failed(before.path, timedelta(0))
        self.assertEqual(self.jar.queue_length(), 1)
        after = self.jar.get_next_for_processing()
        self.assertEqual(self.jar.queue_length(), 0)
        self.assertEqual(before, after)
        self.assertEquals(self.eg_listener.call_count, 1)

    def test08_fail_delayed(self):
        '''
        CookieJar Sequence: Enrich -> Get Next -> Mark Failed 3s Delay -> Queue Empty Until Delay
        '''
        self.jar.enrich_cookie(self.eg_paths[0], self.eg_enrichments[0])
        to_process = self.jar.get_next_for_processing()

        self.jar.mark_as_failed(to_process.path, timedelta(seconds=3))
        self.assertEqual(self.jar.queue_length(), 0)

        # +1 second
        self._change_time(123457)
        self.assertEqual(self.jar.queue_length(), 0)

        # +2 seconds
        self._change_time(123458)
        self.assertEqual(self.jar.queue_length(), 0)

        # +3 seconds
        self._change_time(123459)
        self.assertEqual(self.jar.queue_length(), 1)

        self.assertEquals(self.eg_listener.call_count, 1)

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
        self.assertEquals(self.eg_listener.call_count, 2)

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
        self.assertEquals(self.eg_listener.call_count, 2)


class TestBiscuitTin(TestCookieJar):
    """
    Tests for `BiscuitTin`.
    """
    def setUp(self):
        self.couchdb_container = CouchDBContainer()
        self.HOST = self.couchdb_container.couchdb_fqdn
        self.DB = "cookiejar-test"
        super().setUp()

    def _create_cookie_jar(self) -> BiscuitTin:
        return BiscuitTin(self.HOST, self.DB)

    def _change_time(self, change_time_to: int):
        _dbi._now = MagicMock(return_value=change_time_to)

    def tearDown(self):
        self.couchdb_container.tear_down()

    def test11_connection_failure(self):
        '''
        CookieJar Sequence: Enrich -> Reconnect -> Get Next
        '''
        self.jar.enrich_cookie(self.eg_paths[0], self.eg_enrichments[0])
        new_jar = self._create_cookie_jar()

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

        new_jar = self._create_cookie_jar()
        self.assertEqual(new_jar.queue_length(), 1)

        after = new_jar.get_next_for_processing()
        self.assertEqual(before, after)


class TestInMemoryCookieJar(TestCookieJar):
    """
    Tests for `InMemoryCookieJar`.
    """
    def _create_cookie_jar(self) -> CookieJar:
        return InMemoryCookieJar()

    def _change_time(self, change_time_to: int):
        pass


# Trick required to stop Python's unittest from running the abstract base class as a test
del TestCookieJar


if __name__ == "__main__":
    unittest.main()
