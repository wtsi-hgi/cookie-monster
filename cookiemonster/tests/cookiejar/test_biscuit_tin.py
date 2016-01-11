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
Copyright (c) 2015 Genome Research Limited
'''
import unittest
from unittest.mock import MagicMock

from cookiemonster.common.models import Cookie
from cookiemonster.cookiejar import BiscuitTin
from cookiemonster.cookiejar import CookieJar
from cookiemonster.tests._utils.docker_couchdb import CouchDBContainer
from cookiemonster.tests.cookiejar.test_cookiejar import HiddenTestCookieJar
from cookiemonster.cookiejar import _dbi


class TestBiscuitTin(HiddenTestCookieJar[0]):
    """
    Tests for `BiscuitTin`.
    """
    def setUp(self):
        self.couchdb_container = CouchDBContainer()
        self.HOST = self.couchdb_container.couchdb_fqdn
        self.DB = "cookiejar-test"
        super().setUp()

    def _create_cookie_jar(self) -> CookieJar:
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


if __name__ == "__main__":
    unittest.main()
