"""
HTTP API Test
=============
High-level testing of the HTTP API.

Legalese
--------
Copyright (c) 2016 Genome Research Ltd.

Author: Christopher Harrison <ch12@sanger.ac.uk>

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

from cookiemonster.tests._utils.docker_couchdb import CouchDBContainer
from cookiemonster.tests._utils.docker_helpers import get_open_port

import json
from typing import Any
from time import sleep
from datetime import datetime, timedelta, timezone
from http.client import HTTPConnection, HTTPResponse

from hgicommon.collections import Metadata

from cookiemonster.common.models import Enrichment, Cookie
from cookiemonster.common.helpers import EnrichmentJSONDecoder
from cookiemonster.cookiejar import BiscuitTin
from cookiemonster.elmo import HTTP_API, APIDependency


def _decode_json_response(r:HTTPResponse) -> Any:
    """ Decode JSON HTTP response """
    charset = r.getheader('charset', 'utf-8')

    if r.headers.get_content_type() != 'application/json':
        return None

    return json.loads(r.read().decode(charset))
    

class TestElmo(unittest.TestCase):
    def setUp(self):
        """
        Test set up:

        * Build, if necessary, and start a CouchDB container and
          connect as a BiscuitTin instance
        * Start the HTTP API service on a free port, with the necessary
          dependencies injected
        * Create an HTTP client connection to the API service
        """
        self.couchdb_container = CouchDBContainer()

        # Configuration for Cookie Jar
        self.HOST = self.couchdb_container.couchdb_fqdn
        self.DB = 'elmo-test'

        self.jar = BiscuitTin(self.HOST, self.DB, 1, timedelta(0))

        # Configuration for HTTP service
        self.API_PORT = get_open_port()

        self.api = HTTP_API()
        self.api.inject(APIDependency.CookieJar, self.jar)
        self.api.listen(self.API_PORT)

        self.http = HTTPConnection('localhost', self.API_PORT)
        self.REQ_HEADER = {'Accept': 'application/json'}

        # Block until service is up (or timeout)
        start_time = finish_time = datetime.now()
        service_up = False
        while finish_time - start_time < timedelta(seconds=5):
            response = None

            try:
                self.http.request('HEAD', '/')
                response = self.http.getresponse()

            except:
                sleep(0.1)

            finally:
                self.http.close()
                finish_time = datetime.now()

            if isinstance(response, HTTPResponse):
                service_up = True
                break

        if not service_up:
            self.tearDown()
            raise ConnectionError('Couldn\'t start API service in a reasonable amount of time')

    def tearDown(self):
        """ Tear down test set up """
        self.http.close()
        self.api.stop()
        self.couchdb_container.tear_down()

    def test_queue(self):
        """
        HTTP API: GET /queue
        """
        self.http.request('GET', '/queue', headers=self.REQ_HEADER)
        r = self.http.getresponse()

        self.assertEqual(r.status, 200)
        self.assertEqual(r.headers.get_content_type(), 'application/json')

        data = _decode_json_response(r)
        self.assertIn('queue_length', data)
        self.assertEqual(data['queue_length'], self.jar.queue_length()) # Should be 0

        self.http.close()

        # Add item to the queue
        self.jar.mark_for_processing('/foo')

        self.http.request('GET', '/queue', headers=self.REQ_HEADER)
        data = _decode_json_response(self.http.getresponse())
        self.assertEqual(data['queue_length'], self.jar.queue_length()) # Should be 1

    def test_reprocess(self):
        """
        HTTP API: POST /queue/reprocess
        """
        # Add mocked update notifier to Cookie Jar
        dirty_cookie_listener = MagicMock()
        self.jar.add_listener(dirty_cookie_listener)

        cookie_identifier = '/foo'
        request = {'identifier': cookie_identifier}
        self.http.request('POST', '/queue/reprocess', body=json.dumps(request), headers=self.REQ_HEADER)
        r = self.http.getresponse()

        self.assertEqual(r.status, 200)
        self.assertEqual(r.headers.get_content_type(), 'application/json')

        data = _decode_json_response(r)
        self.assertEqual(data, request)

        self.http.close()

        # Check queue has been updated
        self.assertEqual(self.jar.queue_length(), 1)
        self.assertEqual(dirty_cookie_listener.call_count, 1)

    @staticmethod
    def _url_for_identifier(identifier:str):
        """ URL for identifier """
        if identifier[0] == "/":
            return '/cookiejar?identifier={}'.format(identifier)
        else:
            return '/cookiejar/{}'.format(identifier)

    def _fetch_test(self, identifier:str):
        """ Generic fetch test """
        source = 'foobar'
        timestamp = datetime.now().replace(microsecond=0, tzinfo=timezone.utc)
        metadata = Metadata({'foo': 123, 'bar': 'quux'})
        enrichment = Enrichment(source, timestamp, metadata)

        self.jar.enrich_cookie(identifier, enrichment)

        self.http.request('GET', TestElmo._url_for_identifier(identifier), headers=self.REQ_HEADER)
        r = self.http.getresponse()

        self.assertEqual(r.status, 200)
        self.assertEqual(r.headers.get_content_type(), 'application/json')

        data = _decode_json_response(r)

        fetched_identifier = data['identifier']
        fetched_enrichment = json.loads(json.dumps(data['enrichments']), cls=EnrichmentJSONDecoder)[0]

        self.assertEqual(fetched_identifier, identifier)
        self.assertEqual(fetched_enrichment, enrichment)

    def test_fetch_by_qs(self):
        """
        HTTP API: GET /cookiejar?identifier=<identifier>
        """
        self._fetch_test('/path/to/foo')

    def test_fetch_by_route(self):
        """
        HTTP API: GET /cookiejar/<identifier>
        """
        self._fetch_test('foo_bar')

    def _delete_test(self, identifier:str):
        """ Generic delete test """
        self.jar.mark_for_processing(identifier)
        self.jar.mark_as_complete(identifier)

        cookie = self.jar.fetch_cookie(identifier)
        self.assertIsInstance(cookie, Cookie)

        self.http.request('DELETE', TestElmo._url_for_identifier(identifier), headers=self.REQ_HEADER)
        r = self.http.getresponse()

        self.assertEqual(r.status, 200)
        self.assertEqual(r.headers.get_content_type(), 'application/json')

        data = _decode_json_response(r)
        self.assertEqual(data, {'deleted':identifier})

        deleted_cookie = self.jar.fetch_cookie(identifier)
        self.assertIsNone(deleted_cookie)

    def test_delete_by_qs(self):
        """
        HTTP API: DELETE /cookiejar?identifier=<identifier>
        """
        self._delete_test('/path/to/foo')

    def test_delete_by_route(self):
        """
        HTTP API: DELETE /cookiejar/<identifier>
        """
        self._delete_test('foo_bar')

if __name__ == '__main__':
    unittest.main()
