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

`BiscuitTin` implements `Listenable`; when a cookie is queued (i.e., on
metadata enrichment or exceptional marking), it will broadcast the
updated queue length to all downstream listeners.

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
from time import sleep
from threading import Thread, Lock

from cookiemonster.common.models import Enrichment, Cookie
from cookiemonster.cookiejar._cookiejar import CookieJar
from cookiemonster.cookiejar._dbi import Bert, Ernie


class BiscuitTin(CookieJar):
    ''' Persistent implementation of `CookieJar` '''
    def __init__(self, db_host: str, db_name: str, notify_interval: timedelta=timedelta(minutes=10)):
        '''
        Constructor: Initialise the database connections and the queue
        length change notifier

        @param  db_host          Database host URL
        @param  db_name          Database name
        @param  notify_interval  Queue length notification interval
        '''
        super().__init__()
        self._queue = Bert(db_host, db_name)
        self._metadata = Ernie(db_host, db_name)

        # Queue length change watching
        self._notify_interval = notify_interval.total_seconds()
        self._last_length = 0
        self._watcher = Thread(target=self._broadcast_length_on_change, daemon=True)
        self._watcher.start()

        # Queue lock
        self._qlock = Lock()

    def _broadcast_length_on_change(self):
        # NOTE Because delayed reprocessing requests appear in the queue
        # passively, we have to poll rather than using CouchDB's nifty
        # change API to get changes...thus take "on change" liberally!
        while True:
            sleep(self._notify_interval)
            if self._last_length != self.queue_length():
                self._broadcast_length()

    def _broadcast_length(self):
        '''
        Broadcast the current queue length to all listeners and keep the
        latest broadcast state
        '''
        self._last_length = self.queue_length()
        self.notify_listeners(self._last_length)

    def enrich_cookie(self, path: str, enrichment: Enrichment):
        self._metadata.add_metadata(path, enrichment)
        self._queue.mark_dirty(path)
        self._broadcast_length()

    def mark_as_failed(self, path: str, requeue_delay: Optional[timedelta] = None):
        # NOTE The notification requirement is satisfied by polling in a
        # separate thread with _broadcast_length_on_change
        self._queue.mark_finished(path)
        self._queue.mark_dirty(path, requeue_delay or timedelta())

    def mark_as_complete(self, path: str):
        self._queue.mark_finished(path)

    def mark_for_processing(self, path: str):
        self._queue.mark_dirty(path)
        self._broadcast_length()

    def get_next_for_processing(self) -> Optional[Cookie]:
        with self._qlock:
            to_process = self._queue.dequeue()

        if to_process is None:
            return None

        cookie = Cookie(to_process)
        cookie.enrichments = self._metadata.get_metadata(to_process)

        return cookie

    def queue_length(self) -> int:
        return self._queue.queue_length()
