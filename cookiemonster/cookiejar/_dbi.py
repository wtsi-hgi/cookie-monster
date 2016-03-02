'''
Database Interface
==================
Abstraction layer over a revisionable document-based database (i.e.,
CouchDB)

Exportable classes: `Sofabed`, `Bert`, `Ernie`

`Bert` and `Ernie` are the queue management and metadata repository
database interfaces, respectively. They are two halves of the same
whole, share data together and love each other...which is cool, man.
Which is cool.

Sofabed
-------
`Sofabed` is a CouchDB interface that provides automatic buffering of
inserts and updates. It is instantiated per pycouchdb.client.Server,
with the additional options of maximum buffer size and discharge
latency, and ought to be passed into classes that represent document
models.

Each insert/update will be added to a buffer. If the time between that
and the next is less than the discharge latency, then the next will be
added to the same buffer. This will continue until either the latency
between updates exceeds the discharge latency or the maximum buffer size
(number of documents, rather than bytes): At which point, that buffer
will be discharged into an update queue and ultimately, presuming it's
available, batch pushed into the database.

The Sofabed object will gracefully manage document conflicts and any
connection problems it may have with the database.

Methods:

* `fetch` Fetch a document by its ID and, optionally, revision

* `upsert` Insert or update a document into the database, via the buffer
  and upsert queue; note that buffered and queued documents only exist
  in memory until they are pushed to the database

* `query` Query a predefined view

* `define_view` Define a MapReduce view

Bert and Ernie can share a sofabed (i.e., use the same database), or
sleep separately...it's all part of life's rich tapestry.

Bert (Queue Management DBI)
---------------------------
`Bert` provides an interface with queue management documents on a
CouchDB database and should be instantiated with a Sofabed.

Methods:

* `queue_length` Get the current length of the queue of files to be
  processed

* `mark_dirty` Mark a file as requiring (re)processing, inserting a new
  record if it doesn't already exist, with an optional delay

* `dequeue` Dequeue the next file to process

* `mark_finished` Mark a file as having finished processing

Document schema:

    $queue      boolean  true (i.e., used as a schema classifier)
    location    string   File path
    dirty       boolean  Whether the file needs reprocessing
    processing  boolean  Whether the file is currently being processed
    queue_from  int      Timestamp from when to queue (Unix epoch)

Ernie (Metadata Repository DBI)
-------------------------------
`Ernie` provides an interface with metadata documents on a CouchDB
database and should be instantiated with a Sofabed.

Methods:

* `enrich` Add a metadata enrichment document for a file to the
  repository

* `get_metadata` Fetch all the metadata enrichments for a file, in
  chronological order

Document schema:

    $metadata  boolean  true (i.e., used as a schema classifier)
    location   string   File path
    source     string   Metadata source
    timestamp  int      Timestamp (Unix epoch)
    metadata   object   Key-value store

Dependencies
------------
* pycouchdb
* CouchDB 0.10, or later

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015, 2016 Genome Research Limited
'''

# from json import JSONEncoder
# from time import time, mktime
# from typing import Any, Callable, Iterable, Optional, Generator
# 
# from hgicommon.collections import Metadata
# from cookiemonster.common.models import Enrichment
#
#
#def _now() -> int:
#    '''
#    @return The current Unix time
#    '''
#    return int(time())
#
#
#class _EnrichmentEncoder(JSONEncoder):
#    ''' JSON encoder for Enrichment models '''
#    def default(self, enrichment: Enrichment) -> dict:
#        return {
#            'source':    enrichment.source,
#            'timestamp': int(mktime(enrichment.timestamp.timetuple())),
#            'metadata':  enrichment.metadata._data
#        }

from collections import deque
from copy import deepcopy
from datetime import datetime, timedelta
from threading import Lock, Thread
from time import sleep
from typing import Any, Callable, Generator, Optional
from uuid import uuid4

from requests import head, Timeout

from pycouchdb.client import Server, Database
from pycouchdb.exceptions import NotFound, Conflict

from hgicommon.mixable import Listenable


# TODO Make this configurable?
_COUCHDB_TIMEOUT = timedelta(milliseconds=200)


class _UnresponsiveCouchDB(Exception):
    ''' Unresponsive (i.e., down/busy) database exception '''
    pass


class _ErroneousCouchDB(Exception):
    ''' Erroneous (i.e., bad state) database exception '''
    pass


class _SofterCouchDB(object):
    ''' A CouchDB client interface with a gentle touch '''
    def __init__(self, url:str, database:str, timeout:timedelta, **kwargs):
        '''
        Acquire a connection with the CouchDB database

        @param   url       CouchDB server URL
        @param   database  Database name
        @param   timeout   Connection timeout
        @kwargs  Additional constructor parameters to
                 pycouchdb.client.Server should be passed through here
        '''
        self._url = url
        self._timeout = timeout.total_seconds()

        # Set up pycouchdb constructor arguments and instantiate
        self._server = Server(**{
            'base_url':    url,
            'verify':      False,
            'full_commit': True,
            'authmethod':  'basic',
            **kwargs
        })

        # Connect
        self._db = self._make_it_check(self._connect)(self, database)

        # Monkey-patch the available database methods,
        # decorated with connection checking
        for member in dir(self._db):
            if callable(getattr(self._db, member)) and not member.startswith('_'):
                setattr(self.__class__, member, self._make_it_check(getattr(self._db, member)))

    def _connect(self, database:str) -> Database:
        '''
        Connect to (or create, if it doesn't exist) a database

        @param   database  Database name
        @return  Database object
        '''
        try:
            db = self._server.database(database)
        except NotFound:
            db = self._server.create(database)

        return db

    def _make_it_check(self, fn:Callable[..., Any]) -> Callable[..., Any]:
        '''
        Decorator that checks the database connection before executing
        the function; if the connection is unresponsive or otherwise
        invalid, an exception is thrown instead

        @param   fn  Function to decorate
        @return  Decorated function
        '''
        def wrapper(cls, *args, **kwargs):
            try:
                # FIXME? What about authenticated services?
                response = head(self._url, timeout=self._timeout)
            except Timeout:
                raise _UnresponsiveCouchDB
            except:
                raise _ErroneousCouchDB

            if response.status_code != 200:
                raise _ErroneousCouchDB

            return fn(*args, **kwargs)

        return wrapper


class _UpsertBuffer(Listenable):
    ''' Document buffer '''
    def __init__(self, max_size:int = 100, latency:timedelta = timedelta(milliseconds=50)):
        super().__init__()

        if max_size == 0 or latency == timedelta(0):
            raise TypeError('Buffer must have a non-trivial maximum size and latency')

        self._max_size = max_size
        self._latency = latency

        self._lock = Lock()
        self.data = []
        self.last_updated = datetime.now()

        # Start the watcher
        self._watcher_thread = Thread(target=self._watcher, daemon=True)
        self._thread_running = True
        self._watcher_thread.start()

    def __del__(self):
        self._thread_running = False

    def _discharge(self):
        '''
        Discharge the buffer (by pushing the data to listeners) when its
        state requires so; i.e., when there is something listening and
        either the buffer size or latency is exceeded
        '''
        with self._lock:
            if len(self._listeners) and len(self.data):
                latency = datetime.now() - self.last_updated
                if len(self.data) >= self._max_size or latency >= self._latency:
                    self.notify_listeners(deepcopy(self.data))
                    self.data[:] = []
                    self.last_updated = datetime.now()

    def _watcher(self):
        ''' Watcher thread for time-based discharging '''
        zzz = self._latency.total_seconds() / 2

        while self._thread_running:
            self._discharge()
            sleep(zzz)

    def append(self, document:dict):
        ''' Add a document to the buffer '''
        with self._lock:
            self.data.append(document)
            self.last_updated = datetime.now()

        # Force size-based discharging
        self._discharge()


class Sofabed(object):
    ''' Buffered, append-optimised CouchDB interface '''
    def __init__(self, url:str, database:str, max_buffer_size:int = 100,
                                              buffer_latency:timedelta = timedelta(milliseconds=50),
                                              **kwargs):
        '''
        Acquire a connection with the CouchDB server and initialise the
        buffering queue

        @param   url              CouchDB server URL
        @param   database         Database name
        @param   max_buffer_size  Maximum buffer size (no. of documents)
        @param   buffer_latency   Buffer latency before discharge
        @kwargs  Additional constructor parameters to
                 pycouchdb.client.Server should be passed through here
        '''
        self._db = _SofterCouchDB(url, database, _COUCHDB_TIMEOUT, **kwargs)

        self._queue_lock = Lock()
        self._upsert_queue = deque()

        self._buffer_lock = Lock()
        self._buffer = _UpsertBuffer(max_buffer_size, buffer_latency)
        self._buffer.add_listener(self._enqueue_buffer)

        self._watcher_thread = Thread(target=self._watcher, daemon=True)
        self._thread_running = True
        self._watcher_thread.start()

    def __del__(self):
        self._thread_running = False

    def _enqueue_buffer(self, data:Any):
        '''
        Enqueue discharged buffer data to the upsert queue; should be
        used as the buffer's listener

        @param   data  Buffer data

        WARNING The upsert queue only exists in memory; if the
        application were to fail, the discharged buffers that are
        waiting would be lost
        '''
        with self._queue_lock:
            self._upsert_queue.append(data)

        # Force upsert attempt
        self._upsert_from_queue()

    def _upsert_from_queue(self):
        '''
        Attempt to dequeue and upsert the data from the upsert queue
        into CouchDB; if the upsert fails, then the data is requeued
        '''
        with self._queue_lock:
            if len(self._upsert_queue):
                documents = self._upsert_queue.popleft()

                # Get revision IDs of documents
                document_ids = [x['_id'] for x in documents]
                revision_ids = {
                    x['id']: x['value']['rev']
                    for x in self._db.all(keys=document_ids, include_docs=False)
                    if 'id' in x
                }

                # Merge revision IDs to avoid resource conflicts
                to_upload = deepcopy(documents)
                for document in to_upload:
                    if document['_id'] in revision_ids:
                        document['_rev'] = revision_ids[document['_id']]

                # Upload, or requeue on failure
                try:
                    _ = self._db.save_bulk(to_upload)
                    # TODO? Requeue difference, if there is any
                except _UnresponsiveCouchDB:
                    self._upsert_queue.appendleft(documents)

    def _watcher(self):
        ''' Periodically check the queue for pending upserts '''
        zzz = _COUCHDB_TIMEOUT.total_seconds() * 2

        while self._thread_running:
            self._upsert_from_queue()
            sleep(zzz)

    def fetch(self, key:str, revision:Optional[str] = None) -> Optional[dict]:
        '''
        Get a database document by its ID and, optionally, revision

        @param   key       Document ID
        @param   revision  Revision ID
        @return  Database document (or None, if not found)
        '''
        try:
            if not revision:
                output = self._db.get(key)

            else:
                output = next((
                    doc
                    for doc in self._db.revisions(key)
                    if  doc['_rev'] == revision
                ), None)

        except NotFound:
            output = None

        return output

    def upsert(self, data:dict, key:Optional[str] = None):
        '''
        Upsert document into CouchDB, via the buffer and upsert queue

        @param   data  Document data
        @param   key   Document ID

        NOTE If the document ID is not provided and the document data
        does not contain an '_id' member, then a key will be generated
        '''
        with self._buffer_lock:
            if '_rev' in data:
                del data['_rev']

            self._buffer.append({'_id':key or uuid4().hex, **data})

    def query(self, design:str, view:str, wrapper:Optional[Callable[[dict], Any]] = None, **kwargs) -> Generator:
        '''
        Query a predefined view

        @param   design   Design document name
        @param   view     View name
        @param   wrapper  Wrapper function applied over result rows
        @kwargs  Query string options for CouchDB
        @return  Results generator
        '''
        # Check view exists
        doc = self.fetch('_design/{}'.format(design))
        if not doc or 'views' not in doc or view not in doc['views']:
            raise NotFound

        view_name = '{}/{}'.format(design, view)
        return self._db.query(view_name, wrapper=wrapper, **kwargs)

    def define_view(self, design:str, view:str, map_fn:str, reduce_fn:Optional[str] = None, language:Optional[str] = 'javascript'):
        '''
        Define and upsert a MapReduce view

        @param   design     Design document name
        @param   view       View name
        @param   map_fn     Map function
        @param   reduce_fn  Reduce function (optional)
        @param   language   MapReduce function language (optional)
        '''
        design_doc_id = '_design/{}'.format(design)

        design_doc = {
            **(self.fetch(design_doc_id) or {}),
            '_id': design_doc_id,
            'language': language
        }

        if 'views' not in design_doc:
            design_doc['views'] = {}

        design_doc['views'][view] = {'map': map_fn}
        if reduce_fn:
            design_doc['views'][view]['reduce'] = reduce_fn

        self.upsert(design_doc)


#class Bert(object):
#    ''' Interface to the queue database documents '''
#    @staticmethod
#    def _reset_processing(query_row: Row) -> Document:
#        '''
#        Wrapper function to query that converts returned result rows
#        into documents with their processing state reset
#        '''
#        doc = Document(query_row['value'])
#        doc['dirty']      = True
#        doc['processing'] = False
#        doc['queue_from'] = _now()
#
#        return doc
#
#    def __init__(self, host: str, database: str):
#        '''
#        Constructor: Connect to the database and create, wherever
#        necessary, the views and update handlers to provide the queue
#        management interface
#
#        @param  host      CouchDB host URL
#        @param  database  Database name
#        '''
#        self.db = _Couch(host, database)
#        self._define_schema()
#        self.db.connect()
#
#        # If there are any files marked as currently processing, this
#        # must be due to a previous failure. Reset all of these for
#        # immediate reprocessing
#        in_progress = self.db.query('queue', 'in_progress', Bert._reset_processing, reduce=False)
#        if len(in_progress):
#            # Use bulk update (i.e., single HTTP request) rather than
#            # invoking update handlers for each
#            self.db._db.update(in_progress.rows)
#
#    def _get_id(self, path: str) -> Optional[str]:
#        '''
#        Get queue document ID by file path
#
#        @param  path  File path
#        '''
#        results = self.db.query('queue', 'get_id', key=path, reduce=False)
#        return results.rows[0].value if len(results) else None
#
#    def queue_length(self) -> int:
#        '''
#        @return The current (for-processing) queue length
#        '''
#        results = self.db.query('queue', 'to_process', endkey = _now(),
#                                                       reduce = True,
#                                                       group  = False)
#
#        return results.rows[0].value if len(results) else 0
#
#    def mark_dirty(self, path: str, lead_time: timedelta = None):
#        '''
#        Mark a file as requiring, potentially delayed, (re)processing
#
#        @param  path       File path
#        @param  lead_time  Requeue lead time
#        '''
#        # Get document ID
#        doc_id = self._get_id(path)
#
#        if doc_id:
#            # Determine queue time
#            queue_from = _now()
#            if lead_time:
#                queue_from += lead_time.total_seconds()
#
#            self.db.upsert('queue', 'set_state', doc_id, dirty      = True,
#                                                         queue_from = queue_from)
#        else:
#            self.db.upsert('queue', 'set_state', location   = path,
#                                                 queue_from = _now())
#
#    def dequeue(self) -> Optional[str]:
#        '''
#        Get the next document on the queue and mark it as processing
#
#        @return File path (None, if empty queue)
#        '''
#        results = self.db.query('queue', 'to_process', endkey = _now(),
#                                                       reduce = False,
#                                                       limit  = 1)
#        if len(results):
#            latest    = results.rows[0]
#            key, path = latest.id, latest.value
#
#            self.db.upsert('queue', 'set_processing', key, processing=True)
#            return path
#    
#    def mark_finished(self, path: str):
#        '''
#        Mark a file as finished processing
#
#        @param  path  File path
#        '''
#        # Get document ID
#        doc_id = self._get_id(path)
#
#        if doc_id:
#            self.db.upsert('queue', 'set_processing', doc_id, processing=False)
#
#    def _define_schema(self):
#        ''' Define views and update handlers '''
#        # View: queue/to_process
#        # Queue documents marked as dirty and not currently processing
#        # Keyed by `queue_from`, set the endkey in queries appropriately
#        # Reduce to the number of items in the queue
#        self.db.define_view('queue', 'to_process',
#            map_fn = '''
#                function(doc) {
#                    if (doc.$queue && doc.dirty && !doc.processing) {
#                        emit(doc.queue_from, doc.location);
#                    }
#                }
#            ''',
#            reduce_fn = '_count'
#        )
#
#        # View: queue/in_progress
#        # Queue documents marked as currently processing
#        self.db.define_view('queue', 'in_progress',
#            map_fn = '''
#                function(doc) {
#                    if (doc.$queue && doc.processing) {
#                        emit(doc._id, doc)
#                    }
#                }
#            '''
#        )
#
#        # View: queue/get_id
#        # Queue documents, keyed by their file path
#        self.db.define_view('queue', 'get_id',
#            map_fn = '''
#                function (doc) {
#                    if (doc.$queue) {
#                        emit(doc.location, doc._id);
#                    }
#                }
#            '''
#        )
#
#        # Update handler: queue/set_state
#        # Create a new queue document (expects `location` in the query
#        # string), or update a queue document's dirty status (expects
#        # `dirty` in the query string and an optional `queue_from`)
#        self.db.define_update('queue', 'set_state',
#            handler_fn = '''
#                function(doc, req) {
#                    var q   = req.query || {},
#                        now = parseInt(q.queue_from || (Date.now() / 1000), 10);
#
#                    // Create new item in queue
#                    if (!doc && req.id && 'location' in q) {
#                        return [{
#                            _id:        req.id,
#                            $queue:     true,
#                            location:   q.location,
#                            dirty:      true,
#                            processing: false,
#                            queue_from: now
#                        }, 'created'];
#                    }
#
#                    // Update
#                    if (doc.$queue && 'dirty' in q) {
#                        doc.dirty      = (q.dirty == 'true');
#                        doc.queue_from = doc.dirty ? now : null;
#
#                        return [doc, 'changed'];
#                    }
#
#                    // Failure
#                    return [null, 'failed'];
#                }
#            '''
#        )
#
#        # Update handler: queue/set_processing
#        # Update a queue document's processing status
#        self.db.define_update('queue', 'set_processing',
#            handler_fn = '''
#                function(doc, req) {
#                    var q = req.query || {};
#
#                    // Update
#                    if (doc && doc.$queue && 'processing' in q) {
#                        doc.processing = (q.processing == 'true');
#                        if (doc.processing) {
#                            doc.dirty      = false;
#                            doc.queue_from = null;
#                        }
#
#                        return [doc, 'changed'];
#                    }
#
#                    // Failure
#                    return [null, 'failed'];
#                }
#            '''
#        )
#
#
#class Ernie(object):
#    ''' Interface to the metadata database documents '''
#    def __init__(self, host: str, database: str):
#        '''
#        Constructor: Connect to the database and create, wherever
#        necessary, the views and update handlers to provide the metadata
#        repository interface
#
#        @param  host      CouchDB host URL
#        @param  database  Database name
#        '''
#        self.db = _Couch(host, database)
#        self._define_schema()
#        self.db.connect()
#
#    def enrich(self, path: str, metadata: Enrichment):
#        '''
#        Add a metadata enrichment document to the repository for a file
#
#        @param  path      File path
#        @param  metadata  Metadata model
#        '''
#        req_body = _EnrichmentEncoder().encode(metadata)
#        self.db.upsert('metadata', 'append', location=path, body=req_body)
#
#    def get_metadata(self, path: str) -> Iterable:
#        '''
#        Get all the collected enrichments for a file
#
#        @param  path  File path
#        @return Iterable[Enrichment]
#        '''
#        results = self.db.query('metadata', 'collate', key=path, reduce=False)
#
#        output = [
#            Enrichment(source    = enrichment.value['source'],
#                       timestamp = datetime.fromtimestamp(enrichment.value['timestamp']),
#                       metadata  = Metadata(enrichment.value['metadata']))
#            for enrichment in results
#        ]
#
#        return sorted(output)
#
#    def _define_schema(self):
#        ''' Define views and update handlers '''
#        # View: metadata/collate
#        # Metadata (Enrichment) documents keyed by `location`
#        self.db.define_view('metadata', 'collate',
#            map_fn = '''
#                function(doc) {
#                    if (doc.$metadata) {
#                        emit(doc.location, {
#                            source:    doc.source,
#                            timestamp: doc.timestamp,
#                            metadata:  doc.metadata
#                        });
#                    }
#                }
#            '''
#        )
#
#        # Update handler: metadata/append
#        # Create a new metadata document from POST body, which is
#        # expected to be a JSON object containing `source`, `timestamp`
#        # and `metadata` members, assigned to the `location` from the
#        # query string
#        self.db.define_update('metadata', 'append',
#            handler_fn = '''
#                function(doc, req) {
#                    var q = req.query || {};
#
#                    // Create a new item in the repository
#                    if (!doc && req.id && 'location' in q) {
#                        var req_data,
#                            failure = false;
#
#                        // Parse request body
#                        try      { req_data = JSON.parse(req.body); }
#                        catch(e) { failure = true; }
#
#                        // Check keys exist
#                        if (req_data) {
#                            try {
#                                failure = !('source'    in req_data) ||
#                                          !('timestamp' in req_data) ||
#                                          !('metadata'  in req_data) ||
#                                          !(req_data.metadata instanceof Object);
#                            }
#                            catch(e) {
#                                failure = true;
#                            }
#                        }
#
#                        if (!failure) {
#                            return [{
#                                _id:       req.id,
#                                $metadata: true,
#                                location:  q.location,
#                                source:    req_data.source,
#                                timestamp: req_data.timestamp,
#                                metadata:  req_data.metadata
#                            }, 'created'];
#                        }
#                    }
#
#                    // Failure
#                    return [null, 'failed'];
#                }
#            '''
#        )
