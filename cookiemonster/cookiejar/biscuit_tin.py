"""
Cookie Jar
==========
An implementation of `CookieJar` using CouchDB as its database

Exportable classes: `BiscuitTin`, `RateLimitedBiscuitTin`
Exportable functions: `add_couchdb_logging`

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

add_couchdb_logging
-------------------
Inject CouchDB response time logging into an instantiated `BiscuitTin`

Bert and Ernie
--------------
`_Bert` and `_Ernie` are the queue management and metadata repository
database interfaces, used by `BiscuitTin`, respectively. They are two
halves of the same whole, share data together and love each other...
Which is cool, man. Which is cool.

In `BiscuitTin`, they share a `Sofabed` instance (i.e., use the same
database), but they can be made to sleep separately...it's all part of
life's rich tapestry.

`_Bert` (queue management DBI) methods:

* `get_by_identifier` Get a Cookie by its identifier

* `queue_length` Get the current length of the queue of files to be
  processed

* `mark_dirty` Mark a file as requiring (re)processing, inserting a new
  record if it doesn't already exist, with an optional delay

* `dequeue` Dequeue the next file to process

* `mark_finished` Mark a file as having finished processing

* `delete` Remove a file's queue state, or mark it for deletion if
  currently processing

Document schema:

    $queue      boolean  true (i.e., used as a schema classifier)
    identifier  string   File identifier
    dirty       boolean  Whether the file needs reprocessing
    processing  boolean  Whether the file is currently being processed
    deleted     boolean  Whether the file has been deleted
    queue_from  int      Timestamp from when to queue (Unix epoch)

`_Ernie` (metadata repository DBI) methods:

* `enrich` Add a metadata enrichment document for a file to the
  repository

* `get_metadata` Fetch all the metadata enrichments for a file, in
  chronological order

* `delete_metadata` Delete all the metadata enrichments for a file

Document schema:

    $metadata   boolean  true (i.e., used as a schema classifier)
    identifier  string   File identifier
    source      string   Metadata source
    timestamp   int      Timestamp (Unix epoch)
    metadata    object   Key-value store

Legalese
--------
Copyright (c) 2015, 2016 Genome Research Ltd.

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
import json
import logging
from collections import deque
from datetime import timedelta
from functools import wraps
from os import environ
from threading import Timer
from time import sleep, time
from typing import Any, Callable, Iterable, List, Optional, Tuple

from cookiemonster.common.collections import EnrichmentCollection
from cookiemonster.common.helpers import EnrichmentJSONEncoder, EnrichmentJSONDecoder
from cookiemonster.common.models import Enrichment, Cookie
from cookiemonster.logging.logger import Logger
from cookiemonster.cookiejar._rate_limiter import rate_limited
from cookiemonster.cookiejar.cookiejar import CookieJar
from cookiemonster.cookiejar.couchdb import Actions, Sofabed, inject_logging
from hgicommon.threading import CountingLock


def _now() -> int:
    """ @return The current Unix time """
    return int(time())


# Use the same CouchDB hammering configuration from the environment as
# used by the softer client...with the difference that we don't give up
_COUCHDB_GRACE = timedelta(milliseconds=int(environ.get('COOKIEMONSTER_COUCHDB_GRACE', 1000))).total_seconds()

def _just_keep_swimming(fn:Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator that keeps retrying the method until it executes without
    exception. This is obviously a bit of a hack and should be disabled
    when debugging. However, in practice, CouchDB can be flaky and fail
    for reasons we can't control; but it recovers and so this decorator
    allows us to recover in kind.

    It's similar to hgicommon.decorators.too_big_to_fail, but is per
    function and doesn't involve any shoddy metaprogramming black magic!
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        it_worked = False

        while not it_worked:
            try:
                output = fn(*args, **kwargs)
                it_worked = True

            except:
                logging.exception('%s failed!! Retrying...', fn.__name__)
                sleep(_COUCHDB_GRACE)

        return output

    return wrapper


class _Bert(object):
    """ Interface to the queue database documents """
    @staticmethod
    def _reset_processing(row:dict) -> dict:
        """
        Wrapper function that resets the processing state of the results
        returned from the queue/in_progress view
        """
        return {
            **row['doc'],
            'dirty':      True,
            'processing': False,
            'queue_from': _now()
        }

    def __init__(self, sofa:Sofabed):
        """
        Constructor: Create/update the views to provide the queue
        management interface

        @param   sofa  Sofabed object
        """
        self._db = sofa
        logging.debug('Initialising CouchDB queue management schema')
        self._define_schema()

        # Document schema, with defaults
        self._schema = {
            '$queue':     True,
            'identifier': None,
            'dirty':      False,
            'processing': False,
            'deleted':    False,
            'queue_from': None
        }

        # If there are any files marked as currently processing, this
        # must be due to a previous failure. Reset all of these for
        # immediate reprocessing
        in_progress = self._db.query('queue', 'in_progress', wrapper      = _Bert._reset_processing,
                                                             include_docs = True,
                                                             reduce       = False)
        unclean_restart = False
        for doc in in_progress:
            unclean_restart = True
            self._db.upsert(doc)

        # If there are any files marked as deleted, they can be cleaned
        # up without consequence
        to_delete = self._db.query('queue', 'to_clean', flat='value', reduce=False)

        for doc_id in to_delete:
            unclean_restart = True
            self._db.delete(doc_id)

        if unclean_restart:
            logging.info('Queue state sanitised after unclean restart')

    @_just_keep_swimming
    def get_by_identifier(self, identifier:str) -> Optional[Tuple[str, dict]]:
        """
        Get queue document by its file identifier

        @param   identifier  File identifier
        @return  Document ID and Document tuple (None, if not found)
        """
        results = self._db.query('queue', 'get_id', key          = identifier,
                                                    include_docs = True,
                                                    reduce       = False)
        try:
            result = next(results)
            return result['value'], result['doc']

        except StopIteration:
            return None

    @_just_keep_swimming
    def delete(self, identifier:str):
        """
        Delete a queue document, or mark it for deletion if it is
        currently being processed

        @param   identifier  File identifier
        """
        doc_id, current_doc = self.get_by_identifier(identifier) or (None, None)

        if doc_id:
            if current_doc['processing']:
                deleted_doc = {
                    **current_doc,
                    'deleted': True
                }
                self._db.upsert(deleted_doc)
            else:
                self._db.delete(doc_id)

    @_just_keep_swimming
    def queue_length(self) -> int:
        """
        @return The current (for-processing) queue length
        """
        results = self._db.query('queue', 'to_process', endkey = _now(),
                                                        reduce = True,
                                                        group  = False)
        try:
            return next(results)['value']

        except StopIteration:
            return 0

    @_just_keep_swimming
    def mark_dirty(self, identifier:str, latency:Optional[timedelta] = None):
        """
        Mark a file as requiring, potentially delayed, (re)processing,
        resetting any deleted status

        @param  identifier  File identifier
        @param  latency     Requeue latency
        """
        # Get document, or define minimal default
        doc_id, current_doc = self.get_by_identifier(identifier) or (None, {'identifier': identifier})

        dirty_doc = {
            **self._schema,
            **current_doc,
            'dirty':      True,
            'deleted':    False,
            'queue_from': _now()
        }

        # Latency is only for existing documents
        if doc_id and latency:
            dirty_doc['queue_from'] += latency.total_seconds()

        self._db.upsert(dirty_doc)

    @_just_keep_swimming
    def dequeue(self, count:int) -> List[str]:
        """
        Fetch up to count documents (IDs) off the queue and mark them
        as being processed

        @param   count  The maximum number of documents to dequeue
        @return  List (potentially empty) of dequeued document IDs
        """
        results = self._db.query('queue', 'to_process', endkey       = _now(),
                                                        include_docs = True,
                                                        reduce       = False,
                                                        limit        = count)
        output = []

        for found in results:
            identifier, doc_data = found['value'], found['doc']

            processing_doc = {
                **doc_data,
                'dirty':      False,
                'processing': True,
                'queue_from': None
            }

            self._db.upsert(processing_doc)
            output.append(identifier)

        return output

    @_just_keep_swimming
    def mark_finished(self, identifier:str):
        """
        Mark a file as finished processing, or delete it if it is marked
        as such

        @param  identifier  File identifier
        """
        # Get document
        doc_id, current_doc = self.get_by_identifier(identifier) or (None, None)

        if doc_id:
            if current_doc['deleted']:
                self._db.delete(doc_id)

            else:
                finished_doc = {
                    **current_doc,
                    'processing': False
                }

                self._db.upsert(finished_doc)

    def _define_schema(self):
        """ Define views """
        queue = self._db.create_design('queue')

        # View: queue/to_process
        # Queue documents marked as dirty and not currently processing
        # Keyed by `queue_from`, set the endkey in queries appropriately
        # Reduce to the number of items in the queue
        queue.define_view('to_process',
            map_fn = """
                function(doc) {
                    if (doc.$queue && doc.dirty && !doc.processing && !doc.deleted) {
                        emit(doc.queue_from, doc.identifier);
                    }
                }
            """,
            reduce_fn = '_count'
        )

        # View: queue/in_progress
        # Queue documents marked as currently processing
        queue.define_view('in_progress',
            map_fn = """
                function(doc) {
                    if (doc.$queue && doc.processing && !doc.deleted) {
                        emit(doc.identifier, doc._id)
                    }
                }
            """
        )

        # View: queue/to_clean
        # Queue documents marked for deletion
        queue.define_view('to_clean',
            map_fn = """
                function(doc) {
                    if (doc.$queue && doc.deleted) {
                        emit(doc.identifier, doc._id)
                    }
                }
            """
        )

        # View: queue/get_id
        # Queue documents, keyed by their file identifier
        queue.define_view('get_id',
            map_fn = """
                function (doc) {
                    if (doc.$queue) {
                        emit(doc.identifier, doc._id);
                    }
                }
            """
        )

        self._db.commit_designs()


class _Ernie(object):
    """ Interface to the metadata database documents """
    @staticmethod
    def _to_enrichment(row:dict) -> Enrichment:
        """
        Wrapper function that decodes enrichment data from query result
        rows into its respective Enrichment object
        """
        # FIXME? Annoyingly, we have to re-encode the data back to JSON
        row_json = json.dumps(row['doc'])
        return json.loads(row_json, cls=EnrichmentJSONDecoder)

    def __init__(self, sofa:Sofabed):
        """
        Constructor: Create/update the views to provide the metadata
        repository interface

        @param   sofa  Sofabed object
        """
        self._db = sofa
        logging.debug('Initialising CouchDB metadata repository schema')
        self._define_schema()

        # Document schema, with defaults
        self._schema = {
            '$metadata':  True,
            'identifier': None,
            'source':     None,
            'timestamp':  None,
            'metadata':   {}
        }

    @_just_keep_swimming
    def enrich(self, identifier:str, enrichment:Enrichment):
        """
        Add a metadata enrichment document to the repository for a file

        @param  identifier  File identifier
        @param  enrichment  Enrichment model
        """
        # FIXME? Annoyingly, we have to convert back and forth
        enrichment_dict = json.loads(json.dumps(enrichment, cls=EnrichmentJSONEncoder))

        enrichment_doc = {
            **self._schema,
            **enrichment_dict,
            'identifier': identifier
        }

        self._db.upsert(enrichment_doc)

    @_just_keep_swimming
    def get_metadata(self, identifier:str) -> Iterable:
        """
        Get all the collected enrichments for a file

        @param   identifier  File identifier
        @return  Iterator of Enrichments
        """
        results = self._db.query('metadata', 'collate', wrapper      = _Ernie._to_enrichment,
                                                        key          = identifier,
                                                        include_docs = True,
                                                        reduce       = False)
        return sorted(results)

    @_just_keep_swimming
    def delete_metadata(self, identifier:str):
        """
        Delete all the enrichments for a file

        @param   identifier  File identifier
        """
        to_delete = self._db.query('metadata', 'collate', flat   = 'value',
                                                          key    = identifier,
                                                          reduce = False)

        for doc_id in to_delete:
            self._db.delete(doc_id)

    def _define_schema(self):
        """ Define views """
        metadata = self._db.create_design('metadata')

        # View: metadata/collate
        # Metadata (Enrichment) document IDs keyed by `identifier`
        metadata.define_view('collate',
            map_fn = """
                function(doc) {
                    if (doc.$metadata) {
                        emit(doc.identifier, doc._id);
                    }
                }
            """
        )

        self._db.commit_designs()


class BiscuitTin(CookieJar):
    """ Persistent implementation of `CookieJar` """
    def __init__(self, couchdb_url:str, couchdb_name:str, buffer_capacity:int = 1000,
                                                          buffer_latency:timedelta = timedelta(milliseconds=50),
                                                          **kwargs):
        """
        Constructor: Initialise the database interfaces

        @param  couchdb_url      CouchDB URL
        @param  couchdb_name     Database name
        @param  buffer_capacity  Buffer capacity
        @param  buffer_latency   Buffer latency
        """
        super().__init__()
        self._sofa = Sofabed(couchdb_url, couchdb_name, buffer_capacity, buffer_latency, **kwargs)
        self._queue = _Bert(self._sofa)
        self._metadata = _Ernie(self._sofa)

        self._queue_lock = CountingLock()
        self._pending_cache = deque()

        self._latency = buffer_latency.total_seconds()

    def _broadcast(self):
        """
        Broadcast to all listeners
        This should be called on queue changes
        """
        self.notify_listeners()

    def _get_cookie(self, identifier: str) -> Optional[Cookie]:
        """
        This method *actually* fetches the Cookie, but is not targeted
        by the rate limiter
        """
        _, doc = self._queue.get_by_identifier(identifier) or (None, None)

        if doc is None:
            return None

        cookie = Cookie(identifier)
        cookie.enrichments = EnrichmentCollection(self._metadata.get_metadata(identifier))

        return cookie

    def fetch_cookie(self, identifier: str) -> Optional[Cookie]:
        return self._get_cookie(identifier)

    def delete_cookie(self, identifier: str):
        self._metadata.delete_metadata(identifier)
        self._queue.delete(identifier)

    def enrich_cookie(self, identifier: str, enrichment: Enrichment, mark_for_processing: bool=True):
        self._metadata.enrich(identifier, enrichment)
        if mark_for_processing:
            self._queue.mark_dirty(identifier)
            self._broadcast()

    def mark_as_failed(self, identifier: str, requeue_delay: timedelta=timedelta(0)):
        self._queue.mark_finished(identifier)
        self._queue.mark_dirty(identifier, requeue_delay)
        logging.debug('%s has been marked as failed', identifier)

        # Broadcast the change after the requeue delay
        # FIXME? Timer's interval may not be 100% accurate and may also
        # not correspond with the database server; this could go out of
        # synch... Add a tolerance??
        Timer(requeue_delay.total_seconds(), self._broadcast).start()

    def mark_as_complete(self, identifier: str):
        self._queue.mark_finished(identifier)
        logging.debug('%s has been marked as complete', identifier)

    def mark_for_processing(self, identifier: str):
        self._queue.mark_dirty(identifier)

        self._broadcast()

    def get_next_for_processing(self) -> Optional[Cookie]:
        with self._queue_lock:
            if not self._pending_cache:
                # Dequeue up to as many Cookies (IDs) as there are
                # waiting threads, plus one for the current thread
                waiting = self._queue_lock.waiting_to_acquire()
                logging.debug('Fetching up to %d cookies for processing...', waiting + 1)
                to_process = self._queue.dequeue(waiting + 1)

                if not to_process:
                    return None

                self._pending_cache.extend([
                    self._get_cookie(doc_id)
                    for doc_id in to_process
                ])

            return self._pending_cache.popleft()

    def queue_length(self) -> int:
        return self._queue.queue_length()


@rate_limited
class RateLimitedBiscuitTin(BiscuitTin):
    pass


def add_couchdb_logging(biscuit_tin:BiscuitTin, logger:Logger):
    """
    Inject CouchDB response time logging into an instantiated BiscuitTin

    @param   biscuit_tin  Instantiated BiscuitTin to inject into
    @param   logger       Where to log response times to
    """
    inject_logging(biscuit_tin._sofa._db, logger)

    # Patch in updated decorated function references
    biscuit_tin._sofa._batch_methods = {
        Actions.Upsert: biscuit_tin._sofa._db.save_bulk,
        Actions.Delete: biscuit_tin._sofa._db.delete_bulk
    }
