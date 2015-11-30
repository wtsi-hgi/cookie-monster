'''
Cookie Jar
==========
An implementation of `CookieJar` using CouchDB as its database.

Exportable Classes: `BiscuitTin`, `CookieJar` (by proxy)

BiscuitTin
----------
`BiscuitTin` implements the interface decreed by `CookieJar`. It must be
instantiated with the CouchDB host URL and database name prefix. It will
connect to (and create, wherever necessary) two databases on the
provided host: `{prefix}-queue` and `{prefix}-metadata`, which will
manage the processing queue and metadata for files (using their path as
document IDs), respectively.

`BiscuitTin` implements `Listenable`; when metadata is enriched, it will
broadcast the current queue length to all downstream listeners.

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015 Genome Research Limited
'''

from datetime import timedelta
from typing import Optional

from cookiemonster.common.models import FileUpdate, Enrichment, Cookie
from cookiemonster.cookiejar._cookiejar import CookieJar
from cookiemonster.cookiejar._dbi import Bert, Ernie

class BiscuitTin(CookieJar):
    def __init__(self, db_host: str, db_prefix: str):
        '''
        Constructor: Initialise the database connections

        @param  db_host    Database host URL
        @param  db_prefix  Database name prefix
        '''
        self._queue = Bert(db_host, '{}-queue'.format(db_prefix))
        self._metadata = Ernie(db_host, '{}-metadata'.format(db_prefix))

        # Initialise listeners 
        super().__init__()

    def enrich_cookie(self, path: str, enrichment: Enrichment):
        pass

    def mark_as_failed(self, path: str, requeue_delay: timedelta):
        pass

    def mark_as_complete(self, path: str):
        pass

    def mark_as_reprocess(self, path: str):
        pass

    def get_next_for_processing(self) -> Optional[Cookie]:
        pass

    def queue_length(self) -> int:
        pass
