'''
Cookie Jar
==========
An implementation of `CookieJar` using CouchDB as its database.

Exportable Classes: `BiscuitTin`, `RateLimitedBiscuitTin`

BiscuitTin
----------
`BiscuitTin` implements the interface decreed by `CookieJar`. It must be
instantiated with the buffered CouchDB interface parameters, at which
point it will connect to (and create, if necessary) said database on the
host and set up (again, if necessary) the required design documents to
manage the processing queue and metadata repository.

`BiscuitTin` implements `Listenable`; when a cookie is queued (i.e., on
metadata enrichment or exceptional marking), it will broadcast the
queue change to all downstream listeners.

`RateLimitedBiscuitTin` is a rate-limited version of `BiscuitTin` which
takes an additional argument, at initial position, in its constructor:
`max_requests_per_second`.

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015, 2016 Genome Research Limited
'''
from datetime import timedelta
from typing import Optional
from threading import Lock, Timer

from cookiemonster.common.models import Enrichment, Cookie
from cookiemonster.cookiejar.cookiejar import CookieJar
from cookiemonster.cookiejar._dbi import Sofabed, Bert, Ernie
from cookiemonster.cookiejar._rate_limiter import rate_limited


class BiscuitTin(CookieJar):
    ''' Persistent implementation of `CookieJar` '''
    def __init__(self, couchdb_url:str, couchdb_name:str, buffer_capacity:int = 100,
                                                          buffer_latency:timedelta = timedelta(milliseconds=50)):
        '''
        Constructor: Initialise the database interfaces

        @param  couchdb_url      CouchDB URL
        @param  couchdb_name     Database name
        @param  buffer_capacity  Buffer capacity
        @param  buffer_latency   Buffer latency
        '''
        super().__init__()
        self._sofa = Sofabed(couchdb_url, couchdb_name, buffer_capacity, buffer_latency)
        self._queue = Bert(self._sofa)
        self._metadata = Ernie(self._sofa)

        self._queue_lock = Lock()

    def _broadcast(self):
        '''
        Broadcast to all listeners
        This should be called on queue changes
        '''
        self.notify_listeners()

    def enrich_cookie(self, identifier: str, enrichment: Enrichment):
        self._metadata.enrich(identifier, enrichment)
        self._queue.mark_dirty(identifier)
        self._broadcast()

    def mark_as_failed(self, identifier: str, requeue_delay: timedelta=timedelta(0)):
        self._queue.mark_finished(identifier)
        self._queue.mark_dirty(identifier, requeue_delay)

        # Broadcast the change after the requeue delay
        # FIXME? Timer's interval may not be 100% accurate and may also
        # not correspond with the database server; this could go out of
        # synch... Add a tolerance??
        Timer(requeue_delay.total_seconds(), self._broadcast).start()

    def mark_as_complete(self, identifier: str):
        self._queue.mark_finished(identifier)

    def mark_for_processing(self, identifier: str):
        self._queue.mark_dirty(identifier)
        self._broadcast()

    def get_next_for_processing(self) -> Optional[Cookie]:
        with self._queue_lock:
            to_process = self._queue.dequeue()

        if to_process is None:
            return None

        cookie = Cookie(to_process)
        cookie.enrichments = self._metadata.get_metadata(to_process)

        return cookie

    def queue_length(self) -> int:
        return self._queue.queue_length()


@rate_limited
class RateLimitedBiscuitTin(BiscuitTin):
    pass
