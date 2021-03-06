"""
Dockerised CouchDB Instance
===========================
Build and start a containerised CouchDB instance on an available port
upon instantiation. Exposes the CouchDB host URL, once it has spun up,
and provides a means to tear it down.

Exportable Classes: `CouchDBContainer`

Legalese
--------
Copyright (c) 2015 Genome Research Ltd.

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

import atexit
import logging
from datetime import datetime, timedelta
from http.client import HTTPConnection, HTTPResponse
from os.path import dirname, realpath, join, normpath
from time import sleep
from urllib.parse import urlparse

from docker.client import Client
from docker.utils import kwargs_from_env

from cookiemonster.tests._utils.docker_helpers import get_open_port
from hgicommon.docker.client import create_client

_DOCKERFILE_PATH = normpath(join(dirname(realpath(__file__)),
                                 '../../../docker/couchdb'))
_COUCHDB_IMAGE   = 'hgi/couchdb'


class CouchDBContainer(object):
    def __init__(self):
        """ Build and start the containerised instance """
        self._client = client = create_client()
        self._url = url = urlparse(self._client.base_url)

        self._host = url.hostname if url.scheme in ['http', 'https'] else 'localhost'
        self._port = get_open_port()

        # Exposed interface for CouchDB
        self.couchdb_fqdn = 'http://{host}:{port}'.format(host=self._host, port=self._port)

        # Build image
        # n.b., This will take a while, the first time through
        logging.info('Building CouchDB image...')
        logging.debug([
            line for line in client.build(
                path = _DOCKERFILE_PATH,
                tag  = _COUCHDB_IMAGE
            )
        ])

        # Create container
        self.container = client.create_container(
            image       = _COUCHDB_IMAGE,
            ports       = [self._port],
            host_config = client.create_host_config(
                port_bindings = {5984: self._port}
            )
        )

        # Start container
        logging.info('Starting CouchDB container {Id}...'.format(**self.container))
        logging.debug('Warnings: {Warnings}'.format(**self.container))
        atexit.register(self.tear_down)
        client.start(self.container['Id'])

        # Block 'til 200 OK response received (or timeout)
        test_connection = HTTPConnection(self._host, self._port)
        start_time = finish_time = datetime.now()
        couchdb_started = False
        while finish_time - start_time < timedelta(minutes=1):
            response = None

            try:
                test_connection.request('HEAD', '/')
                response = test_connection.getresponse()

            except:
                sleep(0.1)

            finally:
                test_connection.close()
                finish_time = datetime.now()

            if isinstance(response, HTTPResponse) and response.status == 200:
                couchdb_started = True
                break

        if not couchdb_started:
            self.tear_down()
            raise ConnectionError('Couldn\'t start CouchDB in a reasonable amount of time')

        logging.info('CouchDB container available on {fqdn}'.format(fqdn=self.couchdb_fqdn))

    def tear_down(self):
        """ Tear down the containerised instance """
        if self.container:
            self._client.kill(self.container['Id'])
            self.container = None
