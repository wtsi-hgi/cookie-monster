'''
Cookie Jar
==========
An implementation of `CookieJar` using CouchDB as its database.

Exportable Classes: `BiscuitTin`, `CookieJar` (by proxy)

BiscuitTin
----------
`BiscuitTin` implements the interface decreed by `CookieJar`. It must be
instantiated with the CouchDB host URL and database name. It will
connect to (and create, if necessary) said database on the host and set
up (again, if necessary) the required design documents to manage the
processing queue and metadata repository.

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

from cookiemonster.common.models import Enrichment, Cookie
from cookiemonster.cookiejar._cookiejar import CookieJar
from cookiemonster.cookiejar._dbi import Bert, Ernie


class BiscuitTin(CookieJar):
    """
    Persistent implementation of `CookieJar`.
    """
    def __init__(self, db_host: str, db_name: str):
        '''
        Constructor: Initialise the database connections

        @param  db_host  Database host URL
        @param  db_name  Database name
        '''
        super().__init__()
        self._queue = Bert(db_host, db_name)
        self._metadata = Ernie(db_host, db_name)

    def enrich_cookie(self, path: str, enrichment: Enrichment):
        self._metadata.add_metadata(path, enrichment)
        self._queue.mark_dirty(path)
        self.notify_listeners(self.queue_length())

    def mark_as_failed(self, path: str, requeue_delay: timedelta):
        self._queue.mark_finished(path)
        self._queue.mark_dirty(path, requeue_delay)

    def mark_as_complete(self, path: str):
        self._queue.mark_finished(path)

    def mark_as_reprocess(self, path: str):
        self._queue.mark_dirty(path)

    def get_next_for_processing(self) -> Optional[Cookie]:
        to_process = self._queue.dequeue()

        if to_process is None:
            return None

        cookie = Cookie(to_process)
        cookie.enrichments = self._metadata.get_metadata(to_process)

        return cookie

    def queue_length(self) -> int:
        return self._queue.queue_length()
