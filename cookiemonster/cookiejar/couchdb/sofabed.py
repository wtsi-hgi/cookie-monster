"""
Database Interface
==================
Abstraction layer over a revisionable document-based database (i.e.,
CouchDB), with in-memory caching and buffering to appease the DBA gods

Exportable classes: `Sofabed`

Sofabed
-------
`Sofabed` is a CouchDB interface that provides automatic buffering of
inserts, updates and deletions, while also maintaining an in-memory
cache. It is instantiated per pycouchdb.client.Server (more-or-less),
with the additional options of maximum buffer size and discharge
latency, and ought to be passed into classes that represent document
models.

Each insert, update or deletion will be added to a buffer. If the time
between that and the next is less than the discharge latency, then the
next will be added to the same buffer. This will continue until either
the latency between operations exceeds the discharge latency or the
maximum buffer size (number of documents, rather than bytes) has been
reached: At which point, that buffer will be discharged into an
appropriate queue and ultimately, presuming it's available, batched
against the database.

The Sofabed object will gracefully manage document conflicts and any
connection problems it may have with the database.

Methods:

* `fetch` Fetch a document by its ID and, optionally, revision, checking
  the in-memory cache first

* `upsert` Insert or update a document into the database, via a buffer
  and upsert queue (i.e., in-memory cache)

* `delete` Delete a document from the database, via a buffer and
  deletion queue (i.e., in-memory cache)

* `query` Query a predefined view

* `create_design` Create a new, in-memory design document

* `get_design` Get an in-memory design document by name

* `commit_designs` Commit all in-memory designs to the database

Note that buffered and queued documents only exist in memory until they
are pushed to the database. Data will be lost in the event of failure.

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

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015, 2016 Genome Research Limited
"""
from datetime import timedelta
from typing import Any, Callable, Generator, Optional
from threading import Lock, Thread
from time import sleep
from collections import deque, OrderedDict
from uuid import uuid4

from pycouchdb.exceptions import NotFound

from cookiemonster.cookiejar.couchdb.softer import SofterCouchDB, UnresponsiveCouchDB, InvalidCouchDBKey
from cookiemonster.cookiejar.couchdb.dream_catcher import TODO

class _DesignDocument(object):
    ''' Design document model '''
    def __init__(self, db:SofterCouchDB, name:str, language='javascript'):
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
        self._db = SofterCouchDB(url, database, **kwargs)

        self._designs = []
        self._designs_dirty = False

        # Setup the upsert buffer and queue
        self._upsert_queue_lock = Lock()
        self._upsert_queue = deque()

        self._upsert_buffer_lock = Lock()
        self._upsert_buffer = _DocumentBuffer(max_buffer_size, buffer_latency)
        self._upsert_buffer.add_listener(self._enqueue_buffer)

        # Queue watcher
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
        with self._upsert_queue_lock:
            self._upsert_queue.append(data)

        # Force upsert attempt
        self._upsert_from_queue()

    def _upsert_from_queue(self):
        '''
        Attempt to dequeue and upsert the data from the upsert queue
        into CouchDB; if the upsert fails, then the data is requeued
        '''
        with self._upsert_queue_lock:
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

                except UnresponsiveCouchDB:
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
        zzz = _COUCHDB_TIMEOUT * 2

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
        Upsert document into CouchDB, via the upsert buffer and queue

        @param   data  Document data
        @param   key   Document ID

        NOTE If the document ID is not provided and the document data
        does not contain an '_id' member, then a key will be generated;
        revisions IDs (_rev) are stripped out; and any other CouchDB
        reserved keys (i.e., prefixed with an underscore) will raise an
        exception
        '''
        with self._upsert_buffer_lock:
            if '_rev' in data:
                del data['_rev']

            if any(key.startswith('_') for key in data.keys() if key != '_id'):
                raise InvalidCouchDBKey

            self._upsert_buffer.append({'_id':key or uuid4().hex, **data})

    def delete(self, key:str):
        '''
        Delete document from CouchDB, via the delete buffer and queue

        @param   key  Document ID
        '''
        raise NotImplementedError

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
