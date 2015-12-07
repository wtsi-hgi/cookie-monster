'''
Dockerised CouchDB Instance
===========================

Build and start and containerised CouchDB instance on instantiation,
with methods to get the CouchDB host URL and to tear down the instance.

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
from urllib.parse import urlparse

from docker.client import Client
from docker.utils import kwargs_from_env

_DOCKERFILE_PATH = 'docker/couchdb'
_COUCHDB_IMAGE   = 'hgi/couchdb'

class CouchDBContainer(object):
    def __init__(self):
        ''' Build and start the containerised instance '''
        docker_environment = kwargs_from_env(assert_hostname=False)

        if 'base_url' not in docker_environment:
            raise ConnectionError('Cannot connect to Docker')

        self.client = client = Client(**docker_environment)

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
            image       =  _COUCHDB_IMAGE,
            ports       = [5984],
            host_config = client.create_host_config(
                port_bindings = {5984: 5984}
            )
        )

        # Start container
        logging.info('Starting CouchDB container {Id}...'.format(**self.container))
        logging.debug('Warnings: {Warnings}'.format(**self.container))
        client.start(self.container['Id'])

    def get_couchdb_host(self):
        '''
        Get the Docker daemon host
        n.b., Assume localhost if URL scheme is not TCP

        FIXME Is this right?!
        '''
        url = urlparse(self.client.base_url)
        hostname = url.hostname if url.scheme == 'tcp' else 'localhost'
        return 'http://{hostname}:5984'.format(hostname=hostname)

    def tear_down(self):
        ''' Tear down the containerised instance '''
        self.client.kill(self.container['Id'])
