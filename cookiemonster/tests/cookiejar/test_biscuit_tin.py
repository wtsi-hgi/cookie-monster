'''
Cookie Jar Implementation Test
==============================
High-level integration and logic tests of the CookieJar-CouchDB
implementation (`BiscuitTin`). We assume that if the higher-level tests
pass and are suitably comprehensive, then the underlying levels of
abstraction are probably fine™.

The following sequences are tested:

* Get Next

* Enrich -> Get Next -> Mark Complete

* Enrich 1 -> Enrich 2 -> Get Next (X) -> Get Next (Y) -> Mark X
  Complete -> Mark Y Complete

* Enrich 1 -> Enrich 2 -> Get Next (X) -> Mark X Complete -> Get Next
  (Y) -> Mark Y Complete

* Enrich -> Get Next -> Mark Failed Immediate -> Get Next

* Enrich -> Get Next -> Mark Failed 3s Delay -> Queue empty until delay

* Enrich -> Get Next -> Enrich same -> Mark Complete -> Get Next

* Enrich -> Get Next -> Mark Complete -> Mark Reprocess -> Get Next

* Enrich -> Get Next -> Mark Reprocess -> Mark Complete -> Get Next

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
from cookiemonster.tests.cookiejar._docker import CouchDBContainer

from datetime import datetime

from hgicommon.collections import Metadata
from cookiemonster.common.enums import EnrichmentSource
from cookiemonster.common.models import Enrichment, Cookie

from cookiemonster.cookiejar import BiscuitTin

class TestCookieJar(unittest.TestCase):
    def setUp(self):
        '''
        Build, if necessary, and start a Dockerised CouchDB instance and
        connect. Plus, provide sample inputs with which to test.
        '''
        self.couchdb_container = CouchDBContainer()

        self.HOST = self.couchdb_container.couchdb_fqdn
        self.DB   = 'cookiejar-test'

        self.jar = BiscuitTin(self.HOST, self.DB)

        self.eg_paths       = ['/foo',
                               '/bar/baz']
        self.eg_metadata    = [Metadata({'xyzzy': 123}),
                               Metadata({'quux': 'snuffleupagus'})]
        self.eg_enrichments = [Enrichment('random', datetime.now(), self.eg_metadata[0]),
                               Enrichment(EnrichmentSource.IRODS, datetime.now(), self.eg_metadata[1])]

    def tearDown(self):
        ''' Tear down CouchDB container '''
        self.couchdb_container.tear_down()

    def test_empty_queue(self):
        '''
        Get Next
        '''
        self.assertEqual(self.jar.queue_length(), 0)
        self.assertIsNone(self.jar.get_next_for_processing())

    def test_simple_sequence(self):
        '''
        Enrich -> Get Next -> Mark Complete
        '''
        pass


if __name__ == '__main__':
    unittest.main()
