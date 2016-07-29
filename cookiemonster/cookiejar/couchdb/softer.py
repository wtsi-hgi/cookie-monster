"""
Low-Level CouchDB Interface
===========================
This module provides a wrapper to pycouchdb such that every request made
to the CouchDB server is repeated a set amount of times, after a grace
timeout, before giving up.

Exportable classes: `SofterCouchDB`
Exportable exceptions: `UnresponsiveCouchDB`, `InvalidCouchDBKey`

SofterCouchDB
-------------
`SofterCouchDB` is instantiated with the CouchDB URL and database name,
as well as any additional arguments that you'd normally pass through to
the pycouchdb.client.Server constructor. If the named database is not
found on the CouchDB server, it will be created.

The available methods are a subset of those provided by
pycouchdb.client.Database, using the same calling conventions.

Environmental Factors
---------------------
This CouchDB interface is designed to be more resilient to server
problems, when used in anger, such that data doesn't get lost. This is
parametrised through the following environment variables:

Environment Variable          | Description                    | Default
------------------------------+--------------------------------+--------
COOKIEMONSTER_COUCHDB_GRACE   | Grace time before retry (ms)   |    1000
COOKIEMONSTER_COUCHDB_RETRIES | Maximum retries before failure |       0

n.b., Zero retries means never give up.

Dependencies
------------
* py-couchdb 1.14
* CouchDB 0.10, or later

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

Reimplemented CouchDB client code is based on py-couchdb

Copyright (c) 2016 Andrey Antukh <niwi@niwi.be>

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

1. Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the distribution.

3. The name of the author may not be used to endorse or promote products
   derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
"""
import logging
from datetime import timedelta
from os import environ
from time import sleep
from typing import Optional

# NOTE We rely on undocumented APIs within the base library, hence this
# is fragile wrt version changes...
import pycouchdb

# We get the CouchDB hammering configuration from the environment, as we
# don't want to have to explicitly set every last little thing.
_COUCHDB_GRACE   = timedelta(milliseconds=int(environ.get('COOKIEMONSTER_COUCHDB_GRACE', 1000))).total_seconds()
_COUCHDB_RETRIES = int(environ.get('COOKIEMONSTER_COUCHDB_RETRIES', 0))


class UnresponsiveCouchDB(Exception):
    """ Unresponsive (i.e., down/busy) database exception """
    pass


class InvalidCouchDBKey(Exception):
    """ Invalid (i.e., prefixed with an underscore) key exception """
    pass


class _SofterResource(pycouchdb.resource.Resource):
    """
    Reimplementation of pycouchdb.resource.Resource such that requests
    are gracefully retried up until some limit
    """
    def _keep_trying(self, method:str, path:Optional[str]=None, **kwargs):
        """ Keep requesting in the event of an unknown failure """
        good_response = False
        attempts = 0

        while not good_response and (_COUCHDB_RETRIES == 0 or attempts < _COUCHDB_RETRIES):
            try:
                response = self.request(method, path, **kwargs)
                good_response = True

            except (pycouchdb.exceptions.NotFound, pycouchdb.exceptions.BadRequest):
                # This is a genuine problem, so just reraise
                raise

            except pycouchdb.exceptions.GenericError:
                # This could be due to some kind of transient
                # server/network failure, so retry
                logging.exception('%s request to %s failed!! Retrying...', method, path)

                attempts += 1
                sleep(_COUCHDB_GRACE)

        if not good_response:
            logging.error('Could not make %s request to %s!!', method, path)
            raise UnresponsiveCouchDB

        return response

    def get(self, path:Optional[str] = None, **kwargs):
        """
        Override GET function to keep trying the request
        Modified from pycouchdb.resource.Resource.get
        """
        return self._keep_trying('GET', path, **kwargs)

    def put(self, path:Optional[str] = None, **kwargs):
        """
        Override PUT function to keep trying the request
        Modified from pycouchdb.resource.Resource.put
        """
        return self._keep_trying('PUT', path, **kwargs)

    def post(self, path:Optional[str] = None, **kwargs):
        """
        Override POST function to keep trying the request
        Modified from pycouchdb.resource.Resource.post
        """
        return self._keep_trying('POST', path, **kwargs)

    def delete(self, path:Optional[str] = None, **kwargs):
        """
        Override DELETE function to keep trying the request
        Modified from pycouchdb.resource.Resource.delete
        """
        return self._keep_trying('DELETE', path, **kwargs)

    def head(self, path:Optional[str] = None, **kwargs):
        """
        Override HEAD function to keep trying the request
        Modified from pycouchdb.resource.Resource.head
        """
        return self._keep_trying('HEAD', path, **kwargs)


class _SofterServer(pycouchdb.client.Server):
    """
    Reimplementation of pycouchdb.client.Server using _SofterResource
    """
    def __init__(self, base_url, full_commit=True, authmethod='basic', verify=False):
        """
        Override constructor to use _SofterResource
        Modified from pycouchdb.client.Server.__init__
        """
        self.base_url, credentials = pycouchdb.utils.extract_credentials(base_url)
        self.resource = _SofterResource(self.base_url,
                                        full_commit,
                                        credentials=credentials,
                                        authmethod=authmethod,
                                        verify=verify)


class SofterCouchDB(object):
    """
    A CouchDB client interface with a gentle touch
    
    We specify the exposed database methods, rather than generating them
    at runtime, to avoid too many layers of metaprogramming! For
    convenience sake, only the methods that we use have been specified.
    """
    def __init__(self, url:str, database:str, **kwargs):
        """
        Acquire a connection with the CouchDB database

        @param   url       CouchDB server URL
        @param   database  Database name
        @kwargs  Additional constructor parameters to
                 pycouchdb.client.Server should be passed through here
        """
        # Instantiate pycouchdb Server with our changes
        self._server = _SofterServer(**{
            'base_url':    url,
            'verify':      False,
            'full_commit': True,
            'authmethod':  'basic',
            **kwargs
        })

        # Connect to the database
        try:
            self._db = self._server.database(database)
        except pycouchdb.exceptions.NotFound:
            self._db = self._server.create(database)

    # Exposed pycouchdb.client.Database methods
    # all delete delete_bulk get query revisions save save_bulk

    def all(self, *args, **kwargs):
        logging.debug('pycouchdb.all')
        return self._db.all(*args, **kwargs)

    def delete(self, *args, **kwargs):
        logging.debug('pycouchdb.delete')
        return self._db.delete(*args, **kwargs)
    
    def delete_bulk(self, *args, **kwargs):
        logging.debug('pycouchdb.delete_bulk')
        return self._db.delete_bulk(*args, **kwargs)
    
    def get(self, *args, **kwargs):
        logging.debug('pycouchdb.get')
        return self._db.get(*args, **kwargs)
    
    def query(self, *args, **kwargs):
        logging.debug('pycouchdb.query')
        return self._db.query(*args, **kwargs)
    
    def revisions(self, *args, **kwargs):
        logging.debug('pycouchdb.revisions')
        return self._db.revisions(*args, **kwargs)
    
    def save(self, *args, **kwargs):
        logging.debug('pycouchdb.save')
        return self._db.save(*args, **kwargs)
    
    def save_bulk(self, *args, **kwargs):
        logging.debug('pycouchdb.save_bulk')
        return self._db.save_bulk(*args, **kwargs)
