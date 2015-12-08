'''
Dockerised CouchDB Instance
===========================
Build and start a containerised CouchDB instance on an available port
upon instantiation. Exposes the CouchDB host URL, once it has spun up,
and provides a means to tear it down.

Exportable Classes: `CouchDBContainer`

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015 Genome Research Limited
'''

import logging
import socket
from time import sleep
from datetime import datetime, timedelta
from urllib.parse import urlparse
from http.client import HTTPConnection, HTTPResponse
from os.path import dirname, realpath, join, normpath

from docker.client import Client
from docker.utils import kwargs_from_env

_DOCKERFILE_PATH = normpath(join(dirname(realpath(__file__)),
                                 '../../../docker/couchdb'))
_COUCHDB_IMAGE   = 'hgi/couchdb'

def _get_port():
    ''' Return available port '''
    free_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    free_socket.bind(("", 0))
    free_socket.listen(1)
    port = free_socket.getsockname()[1]
    free_socket.close()
    return port

class CouchDBContainer(object):
    def __init__(self):
        ''' Build and start the containerised instance '''
        docker_environment = kwargs_from_env(assert_hostname=False)

        if 'base_url' not in docker_environment:
            raise ConnectionError('Cannot connect to Docker')

        self._client = client = Client(**docker_environment)
        self._url = url = urlparse(self._client.base_url)

        self._host = url.hostname if url.scheme in ['http', 'https'] else 'localhost'
        self._port = _get_port()

        # Exposed interface for CouchDB
        self.couchdb_fqdn = 'http://{host}:{port}'.format(host=self._host, port=self._port)

        # Build image
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
        client.start(self.container['Id'])

        # Block 'til 200 OK response received (or timeout)
        test_connection = HTTPConnection(self._host, self._port)
        start_time = finish_time = datetime.now()
        couchdb_started = False
        while finish_time - start_time < timedelta(seconds=5):
            response = None

            try:
                test_connection.request('HEAD', '/')
                response = test_connection.getresponse()

            except:
                sleep(0.1)

            finally:
                test_connection.close()
                finish_time = datetime.now()

            if type(response) is HTTPResponse and response.status == 200:
                couchdb_started = True
                break

        if not couchdb_started:
            self.tear_down()
            raise ConnectionError('Couldn\'t start CouchDB in a reasonable amount of time')

        logging.info('CouchDB container available on {fqdn}'.format(fqdn=self.couchdb_fqdn))

    def tear_down(self):
        ''' Tear down the containerised instance '''
        self._client.kill(self.container['Id'])
