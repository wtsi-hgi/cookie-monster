'''
Database Interface
==================
Abstraction layer over a revisionable document-based database (i.e.,
CouchDB), with queue management and metadata repository interfaces

Exportable classes: `Sofabed`, `Bert`, `Ernie`

`Bert` and `Ernie` are the queue management and metadata repository
database interfaces, respectively. They are two halves of the same
whole, share data together and love each other...which is cool, man.
Which is cool.

Sofabed
-------
`Sofabed` is a CouchDB interface that provides automatic buffering of
inserts and updates. It is instantiated per pycouchdb.client.Server
(more-or-less), with the additional options of maximum buffer size and
discharge latency, and ought to be passed into classes that represent
document models.

Each insert/update will be added to a buffer. If the time between that
and the next is less than the discharge latency, then the next will be
added to the same buffer. This will continue until either the latency
between updates exceeds the discharge latency or the maximum buffer size
(number of documents, rather than bytes) has been reached: At which
point, that buffer will be discharged into an update queue and
ultimately, presuming it's available, batch pushed into the database.

The Sofabed object will gracefully manage document conflicts and any
connection problems it may have with the database.

Methods:

* `fetch` Fetch a document by its ID and, optionally, revision

* `upsert` Insert or update a document into the database, via the buffer
  and upsert queue; note that buffered and queued documents only exist
  in memory until they are pushed to the database

* `query` Query a predefined view

* `create_design` Create a new, in-memory design document

* `get_design` Get an in-memory design document by name

* `commit_designs` Commit all in-memory designs to the database

Bert and Ernie can share a sofabed (i.e., use the same database), or
sleep separately...it's all part of life's rich tapestry.

_DesignDocument
---------------
Design documents, managed per the Sofabed.*_design(s) methods, are
represented in-memory via this class. This allows you to build up a
design document in bits and commit it to the database all as one.

Methods:

* `define_view` Define a MapReduce view

Note that design documents are committed to the database on demand and
as needed (i.e., when a change is detected), rather than through any
batching process.

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
import json
from collections import deque, OrderedDict
from copy import deepcopy
from datetime import datetime, timedelta
from threading import Lock, Thread
from time import sleep, time
from typing import Any, Callable, Generator, Iterable, Optional, Tuple
from uuid import uuid4

from requests import head, Timeout

from pycouchdb.client import Server, Database
from pycouchdb.exceptions import NotFound, Conflict

from hgicommon.mixable import Listenable
from hgicommon.collections import Metadata

from cookiemonster.common.models import Enrichment

from hgijson.json.models import JsonPropertyMapping
from hgijson.json.primitive import DatetimeEpochJSONEncoder, DatetimeEpochJSONDecoder
from hgijson.json.builders import MappingJSONEncoderClassBuilder, MappingJSONDecoderClassBuilder


# TODO Make this configurable?
_COUCHDB_TIMEOUT = timedelta(milliseconds=200)


_ENRICHMENT_JSON_MAPPING = [
    JsonPropertyMapping('source',    'source',
                                     object_constructor_parameter_name='source'),
    JsonPropertyMapping('timestamp', 'timestamp',
                                     object_constructor_parameter_name='timestamp',
                                     encoder_cls=DatetimeEpochJSONEncoder,
                                     decoder_cls=DatetimeEpochJSONDecoder),
    JsonPropertyMapping('metadata',  object_constructor_parameter_name='metadata',
                                     object_constructor_argument_modifier=Metadata,
                                     object_property_getter=lambda enrichment: dict(enrichment.metadata.items()))
]

_EnrichmentJSONEncoder = MappingJSONEncoderClassBuilder(Enrichment, _ENRICHMENT_JSON_MAPPING).build()
_EnrichmentJSONDecoder = MappingJSONDecoderClassBuilder(Enrichment, _ENRICHMENT_JSON_MAPPING).build()


def _now() -> int:
    ''' @return The current Unix time '''
    return int(time())


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


class _DesignDocument(object):
    ''' Design document model '''
    def __init__(self, db:_SofterCouchDB, name:str, language='javascript'):
        self._db = db
        self.design_id = '_design/{}'.format(name)

        # Synchronise the design with what already exists
        try:
            current_design = self._db.get(self.design_id)
        except NotFound:
            current_design = {}

        self._design = {
            **current_design,
            '_id': self.design_id,
            'language': language
        }

        self._design_dirty = (self._design != current_design)

    def _commit(self):
        ''' Commit the design to the database, if it has changed '''
        if self._design_dirty:
            do_update = True

            try:
                current_doc = self._db.get(self.design_id)

                # Update, if changed
                self._design['_rev'] = current_doc['_rev']
                do_update = (current_doc != self._design)

            except NotFound:
                # Insert new
                if '_rev' in self._design:
                    del self._design['_rev']

            if do_update:
                self._db.save(self._design)

            self._design_dirty = False

    def define_view(self, name:str, map_fn:str, reduce_fn:Optional[str] = None):
        '''
        Define a MapReduce view

        @param   name       View name
        @param   map_fn     Map function
        @param   reduce_fn  Reduce function (optional)
        '''
        # Ensure the design document has a `views` member
        if 'views' not in self._design:
            self._design['views'] = {}

        self._design['views'][name] = {'map': map_fn}
        if reduce_fn:
            self._design['views'][name]['reduce'] = reduce_fn

        # This is probably true
        self._design_dirty = True


class _UpsertBuffer(Listenable):
    ''' Document buffer '''
    def __init__(self, max_size:int = 100, latency:timedelta = timedelta(milliseconds=50)):
        super().__init__()

        self._max_size = max_size if max_size > 0 else 1
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

        self._designs = []
        self._designs_dirty = False

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
            duplicates_to_requeue = False
            if len(self._upsert_queue):
                documents = self._upsert_queue.popleft()
                to_requeue = []

                # Get unique documents (by ID) and find duplicates
                document_ids = OrderedDict()
                for index, doc in enumerate(documents):
                    doc_id = doc['_id']

                    if doc_id not in document_ids:
                        document_ids[doc_id] = index

                    else:
                        # Duplicates should be requeued (rebuffering,
                        # via upsert, would cause a deadlock)
                        to_requeue.append(doc)

                # Get revision IDs of unique documents
                revision_ids = {
                    x['id']: x['value']['rev']
                    for x in self._db.all(keys=list(document_ids.keys()), include_docs=False)
                    if 'id' in x
                }

                # Unique documents with merged revision IDs where they
                # already exist (to avoid update conflicts)
                to_upload = [
                    {
                        **documents[index],
                        **({'_rev': revision_ids[doc_id]} if doc_id in revision_ids else {})
                    }
                    for doc_id, index in document_ids.items()
                ]

                # Requeue duplicates
                if len(to_requeue):
                    duplicates_to_requeue = True
                    self._upsert_queue.appendleft(to_requeue)

                # Upload, or requeue on failure
                try:
                    _ = self._db.save_bulk(to_upload, transaction=True)
                    # TODO? Requeue on transaction failure

                except _UnresponsiveCouchDB:
                    # We don't want to requeue any revision IDs
                    self._upsert_queue.appendleft([
                        documents[index]
                        for index in document_ids.values()
                    ])

        # This must be outside the lock to avoid deadlock
        # (A reentrant lock wouldn't work in this case)
        if duplicates_to_requeue:
            self._upsert_from_queue()

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

    def create_design(self, name:str) -> _DesignDocument:
        '''
        Append a new design document

        @param   name  Design document name
        @return  The design document
        '''
        new_design = _DesignDocument(self._db, name)
        self._designs.append(new_design)
        self._designs_dirty = True
        return new_design

    def get_design(self, name:str) -> Optional[_DesignDocument]:
        '''
        Get an in-memory design document by name

        @param   name  Design document name
        @return  The design document (None, if not found)
        '''
        design_id = '_design/{}'.format(name)
        return next((
            design
            for design in self._designs
            if  design.design_id == design_id
        ), None)

    def commit_designs(self):
        '''
        Commit all design documents to the database
        '''
        if self._designs_dirty:
            for design in self._designs:
                design._commit()
            self._designs_dirty = False


class Bert(object):
    ''' Interface to the queue database documents '''
    @staticmethod
    def _reset_processing(row:dict) -> dict:
        '''
        Wrapper function that resets the processing state of the results
        returned from the queue/in_progress view
        '''
        return {
            **row['value'],
            'dirty':      True,
            'processing': False,
            'queue_from': _now()
        }

    def __init__(self, sofa:Sofabed):
        '''
        Constructor: Create/update the views to provide the queue
        management interface

        @param   sofa  Sofabed object
        '''
        self._db = sofa
        self._define_schema()

        # Document schema, with defaults
        self._schema = {
            '$queue':     True,
            'location':   None,
            'dirty':      False,
            'processing': False,
            'queue_from': None
        }

        # If there are any files marked as currently processing, this
        # must be due to a previous failure. Reset all of these for
        # immediate reprocessing
        in_progress = self._db.query('queue', 'in_progress', wrapper = Bert._reset_processing,
                                                             reduce  = False)
        for doc in in_progress:
            self._db.upsert(doc)

    def _get_by_path(self, path:str) -> Optional[Tuple[str, dict]]:
        '''
        Get queue document by its file path

        @param   path  File path
        @return  Document ID and Document tuple (None, if not found)
        '''
        results = self._db.query('queue', 'get_id', key          = path,
                                                    include_docs = True,
                                                    reduce       = False)
        try:
            result = next(results)
            return result['value'], result['doc']

        except StopIteration:
            return None

    def queue_length(self) -> int:
        '''
        @return The current (for-processing) queue length
        '''
        results = self._db.query('queue', 'to_process', endkey = _now(),
                                                        reduce = True,
                                                        group  = False)
        try:
            return next(results)['value']

        except StopIteration:
            return 0

    def mark_dirty(self, path:str, latency:Optional[timedelta] = None):
        '''
        Mark a file as requiring, potentially delayed, (re)processing

        @param  path     File path
        @param  latency  Requeue latency
        '''
        # Get document, or define minimal default
        doc_id, current_doc = self._get_by_path(path) or (None, {'location': path})

        dirty_doc = {
            **self._schema,
            **current_doc,
            'dirty': True,
            'queue_from': _now()
        }

        # Latency is only for existing documents
        if doc_id and latency:
            dirty_doc['queue_from'] += latency.total_seconds()

        self._db.upsert(dirty_doc)

    def dequeue(self) -> Optional[str]:
        '''
        Get the next document on the queue and mark it as processing

        @return File path (None, if empty queue)
        '''
        results = self._db.query('queue', 'to_process', endkey       = _now(),
                                                        include_docs = True,
                                                        reduce       = False,
                                                        limit        = 1)
        try:
            latest = next(results)
            path, current_doc = latest['value'], latest['doc']

            processing_doc = {
                **current_doc,
                'dirty':      False,
                'processing': True,
                'queue_from': None
            }

            self._db.upsert(processing_doc)
            return path

        except StopIteration:
            return None

    def mark_finished(self, path:str):
        '''
        Mark a file as finished processing

        @param  path  File path
        '''
        # Get document
        doc_id, current_doc = self._get_by_path(path) or (None, None)

        if doc_id:
            finished_doc = {
                **current_doc,
                'processing': False
            }

            self._db.upsert(finished_doc)

    def _define_schema(self):
        ''' Define views '''
        queue = self._db.create_design('queue')

        # View: queue/to_process
        # Queue documents marked as dirty and not currently processing
        # Keyed by `queue_from`, set the endkey in queries appropriately
        # Reduce to the number of items in the queue
        queue.define_view('to_process',
            map_fn = '''
                function(doc) {
                    if (doc.$queue && doc.dirty && !doc.processing) {
                        emit(doc.queue_from, doc.location);
                    }
                }
            ''',
            reduce_fn = '_count'
        )

        # View: queue/in_progress
        # Queue documents marked as currently processing
        queue.define_view('in_progress',
            map_fn = '''
                function(doc) {
                    if (doc.$queue && doc.processing) {
                        emit(doc._id, doc)
                    }
                }
            '''
        )

        # View: queue/get_id
        # Queue documents, keyed by their file path
        queue.define_view('get_id',
            map_fn = '''
                function (doc) {
                    if (doc.$queue) {
                        emit(doc.location, doc._id);
                    }
                }
            '''
        )

        self._db.commit_designs()


class Ernie(object):
    ''' Interface to the metadata database documents '''
    @staticmethod
    def _to_enrichment(row:dict) -> Enrichment:
        '''
        Wrapper function that decodes enrichment data from the database
        into its respective Enrichment object
        '''
        # Annoyingly, we have to re-encode the data back into JSON
        row_json = json.dumps(row['doc'])
        return json.loads(row_json, cls=_EnrichmentJSONDecoder)

    def __init__(self, sofa:Sofabed):
        '''
        Constructor: Create/update the views to provide the metadata
        repository interface

        @param   sofa  Sofabed object
        '''
        self._db = sofa
        self._define_schema()

        # Document schema, with defaults
        self._schema = {
            '$metadata': True,
            'location':  None,
            'source':    None,
            'timestamp': None,
            'metadata':  {}
        }

    def enrich(self, path:str, enrichment:Enrichment):
        '''
        Add a metadata enrichment document to the repository for a file

        @param  path        File path
        @param  enrichment  Enrichment model
        '''
        # Annoyingly, we have to convert back and forth
        enrichment_dict = json.loads(json.dumps(enrichment, cls=_EnrichmentJSONEncoder))

        enrichment_doc = {
            **self._schema,
            **enrichment_dict,
            'location': path
        }

        self._db.upsert(enrichment_doc)

    def get_metadata(self, path:str) -> Iterable:
        '''
        Get all the collected enrichments for a file

        @param   path  File path
        @return  Iterator of Enrichments
        '''
        results = self._db.query('metadata', 'collate', wrapper      = Ernie._to_enrichment,
                                                        key          = path,
                                                        include_docs = True,
                                                        reduce       = False)
        return sorted(results)

    def _define_schema(self):
        ''' Define views '''
        metadata = self._db.create_design('metadata')

        # View: metadata/collate
        # Metadata (Enrichment) document IDs keyed by `location`
        metadata.define_view('collate',
            map_fn = '''
                function(doc) {
                    if (doc.$metadata) {
                        emit(doc.location, doc._id);
                    }
                }
            '''
        )

        self._db.commit_designs()
