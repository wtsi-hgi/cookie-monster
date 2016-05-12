"""
Low-Level CouchDB Interface
===========================
This module provides a wrapper to pycouchdb such that its methods always
perform a preflight check to test for a responsive CouchDB server

Exportable classes: `SofterCouchDB`
Exportable exceptions: `UnresponsiveCouchDB`, `InvalidCouchDBKey`

SofterCouchDB
-------------
`SofterCouchDB` is instantiated with the CouchDB URL and database name,
as well as any additional arguments that you'd want to pass through to
the pycouchdb.Server constructor. If the named database is not found on
the CouchDB server, it will be created.

The available methods are the same as those of pycouchdb.Database, where
each method will be prefixed with a finite number of requests to the
database URL to check for responsiveness. The extent to which it hammers
the database server is configured via environment variables.

Environmental Factors
---------------------
The CouchDB interface is designed to "test the water" before making any
request. If it's too cold (i.e., a timeout is reached), then the client
will fallback, to avoid hammering the CouchDB server when it is busy,
and retry a given number of times before failing completely. This is
parametrised through the following environment variables:

Environment Variable          | Description                    | Default
------------------------------+--------------------------------+--------
COOKIEMONSTER_COUCHDB_TIMEOUT | Initial request timeout (ms)   |    1500
COOKIEMONSTER_COUCHDB_GRACE   | Grace time before retry (ms)   |    3000
COOKIEMONSTER_COUCHDB_RETRIES | Maximum retries before failure |      10

Dependencies
------------
* pycouchdb
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
"""
from datetime import timedelta
from os import environ
from time import sleep
from typing import Any, Callable

from requests import head

from pycouchdb.client import Server, Database
from pycouchdb.exceptions import NotFound


# We get the CouchDB hammering configuration from the environment, as we
# don't want to have to explicitly set every last little thing.
_COUCHDB_TIMEOUT = timedelta(milliseconds=int(environ.get('COOKIEMONSTER_COUCHDB_TIMEOUT', 1500))).total_seconds()
_COUCHDB_GRACE   = timedelta(milliseconds=int(environ.get('COOKIEMONSTER_COUCHDB_GRACE', 3000))).total_seconds()
_COUCHDB_RETRIES = int(environ.get('COOKIEMONSTER_COUCHDB_RETRIES', 10))


class UnresponsiveCouchDB(Exception):
    """ Unresponsive (i.e., down/busy) database exception """
    pass


class InvalidCouchDBKey(Exception):
    """ Invalid (i.e., prefixed with an underscore) key exception """
    pass


class SofterCouchDB(object):
    """ A CouchDB client interface with a gentle touch """
    def __init__(self, url:str, database:str, **kwargs):
        """
        Acquire a connection with the CouchDB database

        @param   url       CouchDB server URL
        @param   database  Database name
        @kwargs  Additional constructor parameters to
                 pycouchdb.client.Server should be passed through here
        """
        self._url = url

        # Set up pycouchdb constructor arguments and instantiate
        self._server = Server(**{
            'base_url':    url,
            'verify':      False,
            'full_commit': True,
            'authmethod':  'basic',
            **kwargs
        })

        # Connect
        self._db = self._lightly_hammer(self._connect)(self, database)

        # Monkey-patch the available database methods, decorated with
        # initial connection checking and graceful retrying
        db_methods = [
            method
            for method in dir(self._db)
            if  callable(getattr(self._db, method))
            and not method.startswith('_')
        ]

        for method in db_methods:
            setattr(self.__class__, method, self._lightly_hammer(getattr(self._db, method)))

    def _connect(self, database:str) -> Database:
        """
        Connect to (or create, if it doesn't exist) a database

        @param   database  Database name
        @return  Database object
        """
        try:
            db = self._server.database(database)
        except NotFound:
            db = self._server.create(database)

        return db

    def _lightly_hammer(self, fn:Callable[..., Any]) -> Callable[..., Any]:
        """
        Decorator that first checks the responsiveness of the database
        connection before executing the function; if the database is
        unresponsive, then we wait a bit and try again until such time
        as it responds or the failure conditions are met (configured
        using environment variables). If it ultimately fails, then an
        unresponsive CouchDB exception will be raised.

        @param   fn  Function to decorate
        @return  Decorated function
        """
        def wrapper(obj, *args, **kwargs):
            good_connection = False
            attempts = 0

            while not good_connection and attempts < _COUCHDB_RETRIES:
                try:
                    # FIXME? What about authenticated services?
                    response = head(self._url, timeout=_COUCHDB_TIMEOUT)
                    if response.status_code != 200:
                        raise Exception

                    good_connection = True

                except:
                    attempts += 1
                    sleep(_COUCHDB_GRACE)

            if not good_connection:
                raise UnresponsiveCouchDB

            return fn(*args, **kwargs)

        return wrapper
