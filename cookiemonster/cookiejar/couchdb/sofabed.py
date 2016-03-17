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
from copy import deepcopy
from datetime import timedelta
from typing import Any, Callable, Generator, List, Optional
from uuid import uuid4

from pycouchdb.exceptions import NotFound

from cookiemonster.cookiejar.couchdb.softer import SofterCouchDB, UnresponsiveCouchDB, InvalidCouchDBKey
from cookiemonster.cookiejar.couchdb.dream_catcher import Cache, Actions, NotCached


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
                                              max_cache_size:int = 2097152,
                                              **kwargs):
        '''
        Acquire a connection with the CouchDB server and initialise the
        buffering queue

        @param   url              CouchDB server URL
        @param   database         Database name
        @param   max_buffer_size  Maximum buffer size (no. of documents)
        @param   buffer_latency   Buffer latency before discharge
        @param   max_cache_size   Maximum in-memory cache size (bytes)
        @kwargs  Additional constructor parameters to
                 pycouchdb.client.Server should be passed through here
        '''
        self._db = SofterCouchDB(url, database, **kwargs)

        self._designs = []
        self._designs_dirty = False

        # Batch action to DB method mapping
        self._batch_methods = {
            Actions.Upsert: self._db.save_bulk,
            Actions.Delete: self._db.delete_bulk
        }

        # Setup in-memory cache and buffer
        self._cache = Cache(max_buffer_size, buffer_latency, max_cache_size)
        self._cache.add_listener(self._batch)

    def _batch(self, action:Actions, docs:List[dict]):
        '''
        Perform a batch action against the database

        @param   action  Batch action to perform
        @param   docs    Documents to apply the action against
        '''
        to_batch = deepcopy(docs)

        # To avoid conflicts, we must merge in the revision IDs of
        # existing documents
        document_ids = [doc['_id'] for doc in to_batch]
        revision_ids = {
            query_row['id']: query_row['value']['rev']
            for query_row in self._db.all(keys=document_ids, include_docs=False)
            if 'error' not in query_row
        }

        for doc in to_batch:
            if doc['_id'] in revision_ids:
                doc['_rev'] = revision_ids['_id']

        try:
            _ = self._batch_methods[action](docs, transaction=True)

        except UnresponsiveCouchDB:
            self._cache.requeue(action, docs)

    def fetch(self, key:str, revision:Optional[str] = None) -> Optional[dict]:
        '''
        Get a database document by its ID and, optionally, revision

        @param   key       Document ID
        @param   revision  Revision ID
        @return  Database document (or None, if not found)
        '''
        # First try the cache...
        try:
            output = self._cache.fetch(key, revision)

        except NotCached:
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
        Upsert document, via the in-memory cache

        @param   data  Document data
        @param   key   Document ID

        NOTE If the document ID is not provided and the document data
        does not contain an '_id' member, then a key will be generated;
        revisions IDs (_rev) are stripped out; and any other CouchDB
        reserved keys (i.e., prefixed with an underscore) will raise an
        InvalidCouchDBKey exception
        '''
        if '_rev' in data:
            del data['_rev']

        if any(key.startswith('_') for key in data.keys() if key != '_id'):
            raise InvalidCouchDBKey

        self._cache.upsert({'_id':key or uuid4().hex, **data})

    def delete(self, key:str):
        '''
        Delete document from CouchDB, via the in-memory cache

        @param   key  Document ID
        '''
        self._cache.delete(key)

    def query(self, design:str, view:str, wrapper:Optional[Callable[[dict], Any]] = None, **kwargs) -> Generator:
        '''
        Query a predefined view

        @param   design   Design document name
        @param   view     View name
        @param   wrapper  Wrapper function applied over result rows
        @kwargs  Query string options for CouchDB
        @return  Results generator
        '''
        # Check view exists (not done through cache)
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
        ''' Commit all design documents to the database '''
        if self._designs_dirty:
            for design in self._designs:
                design._commit()
            self._designs_dirty = False
