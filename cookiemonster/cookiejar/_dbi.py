'''
Database Interface
==================
Abstraction layer over a revisionable document-based database (i.e.,
CouchDB)

Exportable classes: `Bert`, `Ernie`

`Bert` and `Ernie` are the queue management and metadata repository
database interfaces, respectively. They are two halves of the same
whole, share data together and love each other...which is cool, man.
Which is cool.

_Couch
------
`_Couch` is the base CouchDB class used to provide a common interface
with the database instance. It should be the parent of instantiable
classes, which should define any view and/or update handlers and connect
at instantiation. Upon which, it will acquire a connection and create
the database, if it does not already exist, and create/update the
predefined design documents.

Methods:

* `connect` Connect to the specified CouchDB host and database, creating
  it if it does not exist, and push all predefined design documents

* `fetch` Fetch a document by its ID and, optional, revision

* `upsert` Invoke an update handler to create or update a document (note
  that all update handlers are expected to conform to a consistent
  interface; see below)

* `query` Query a predefined view

* `define_view` Define a MapReduce view

* `define_update` Define an update handler

Child classes are expected to use these functions to build an interface
for a particular type of document.

Update handler interface:

* On creation, the response body will be `created`
* On update, the response body will be `updated` and the revision ID can
  be got from the `X-Couch-Update-NewRev` response header
* On failure, the response body will be `failed`

...it's up to the developer to write the function correctly!

FIXME This is a rather messy abstraction interface! The idea is that
subclasses represent schema'd documents with handy manipulation
functions. The couchdb.mapping stuff is similar...

Bert (Queue Management DBI)
---------------------------
`Bert` provides an interface with queue management documents on a
CouchDB database and should be instantiated with the database host and
name.

Methods:

* `queue_length` Get the current length of the queue of files to be
  processed

* `mark_dirty` Mark a file as requiring (re)processing, inserting a new
  record if it doesn't already exist, with an optional delay

* `dequeue` Dequeue the next file to process

* `mark_finished` Mark a file as having finished processing

Document schema:

    location    string   File path
    queue       boolean  true (i.e., used as a schema classifier)
    dirty       boolean  Whether the file needs reprocessing
    processing  boolean  Whether the file is currently being processed
    queue_from  int      Timestamp from when to queue (Unix epoch)

Ernie (Metadata Repository DBI)
-------------------------------
TODO

Document schema:

    _id        [UUID]     
    _rev       [Metadata revision, by path and source]
    path       File path
    source     Source tag
    timestamp  Timestamp (Unix time)
    metadata   { [metadata key-values] }

Dependencies
------------
* couchdb-python
* CouchDB 0.10, or later

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015 Genome Research Limited
'''

from typing import Any, Optional, Callable
from copy import deepcopy
from datetime import timedelta
from time import time
from uuid import uuid4

import couchdb
from couchdb.client import Server, Document, ViewResults, Row

from hgicommon.collections import Metadata


def _document_to_dictionary(document: Optional[Document]) -> dict:
    '''
    Strip out the CouchDB keys (_id and _rev) from a document and return
    a plain dictionary

    @param  document  CouchDB document
    @return Dictionary
    '''
    if not document:
        return {}
    
    couch_keys = ['_id', '_rev']
    return {
        key: value
        for key, value in document.items()
        if  key not in couch_keys
    }


def _now() -> int:
    '''
    @return The current Unix time
    '''
    return int(time())


class _Couch(object):
    ''' CouchDB abstraction base class '''
    def __init__(self):
        self._couch   = None
        self._db      = None
        self._designs = {}

    def connect(self, host: str, database: str):
        '''
        Acquire connection with the CouchDB host and use (and create)
        the database and any predefined design documents

        @param  host      CouchDB host URL
        @param  database  Database name
        '''
        if not self._db:
            self._couch = Server(host)

            if database not in self._couch:
                self._couch.create(database)

            self._db = self._couch[database]

        self._push_designs()

    def _check_connection(self):
        ''' Raise an exception if there is no connection '''
        if not self._db:
            raise ConnectionError('Not connected to any database')

    def fetch(self, key: str, revision: Optional[str] = None) -> Optional[Document]:
        '''
        Get a database document by its ID and, optionally, revision

        @param  key       Document ID
        @param  revision  Revision ID
        @return Database document (or None, if not found)
        '''
        self._check_connection()
        output = None

        if key in self._db:
            if revision:
                output = next((
                    doc
                    for doc in self._db.revisions(key)
                    if  doc['_rev'] == revision
                ), None)
            else:
                output = self._db[key]

        return output

    def _upsert_raw(self, key: str, data: dict) -> str:
        '''
        Insert or update raw data by ID

        @param  key   Document ID
        @param  data  Data dictionary
        @return The revision ID for the document

        n.b., If the document hasn't changed since the current revision,
        then no insert is performed and that revision's ID will be
        returned

        n.b., The `update` method, which uses a defined update handler,
        should be used in most cases
        '''
        self._check_connection()
        
        new_data = deepcopy(data)

        if key in self._db:
            # Update
            current_doc  = self.fetch(key)
            current_data = _document_to_dictionary(current_doc)
            if new_data != current_data:
                # To avoid update conflicts, we must explicitly set the
                # revision key to the latest
                new_data['_id']  = key
                new_data['_rev'] = current_doc.rev
                _, _rev = self._db.save(Document(new_data))
            else:
                _rev = current_doc.rev

        else:
            # Insert
            new_data['_id'] = key
            _, _rev = self._db.save(Document(new_data))

        return _rev

    def upsert(self, design: str, update: str, key: Optional[str] = None, **options) -> Optional[Document]:
        '''
        Invoke an update handler

        @param  design   Design document name
        @param  view     View name
        @param  key      Document ID to update (None for insert)
        @param  options  Query string options for CouchDB
        @return Updated document (or None, on failure)
        '''
        self._check_connection()

        # Check handler exists
        doc = self.fetch('_design/{}'.format(design))
        if not doc or 'updates' not in doc or update not in doc['updates']:
            return None

        # A new key for a new document
        if not key:
            key = uuid4().hex

        update_name = '{}/{}'.format(design, update)
        res_headers, res_body = self._db.update_doc(update_name, key, **options)

        # Decode the response body
        charset = res_headers.get_content_charset() or 'UTF8'
        response = res_body.getvalue().decode(charset)

        if response == 'created':
            return self.fetch(key)

        elif response == 'updated':
            revision = res_headers.get('X-Couch-Update-NewRev')
            return self.fetch(key, revision)

        else:
            return None

    def query(self, design: str, view: str, wrapper: Optional[Callable[[Row], Any]] = None, **options) -> Optional[ViewResults]:
        '''
        Query a predefined view

        @param  design   Design document name
        @param  view     View name
        @param  wrapper  Wrapper function applied over result rows
        @param  options  Query string options for CouchDB
        @return ViewResults iterable (or None, if no such view)
        '''
        self._check_connection()

        # Check view exists
        doc = self.fetch('_design/{}'.format(design))
        if not doc or 'views' not in doc or view not in doc['views']:
            return None

        view_name = '{}/{}'.format(design, view)
        return self._db.view(view_name, wrapper, **options)

    def _define_design(self, name: str) -> dict:
        '''
        Define a JavaScript design document

        @param  name  Design document name
        '''
        if name not in self._designs:
            self._designs[name] = {'language': 'javascript'}
        return self._designs[name]

    def _push_designs(self):
        ''' Push local design documents to the server '''
        for name, design in self._designs.items():
            # Upsert won't do anything if there are no changes...which
            # is good, as it avoids CouchDB unnecessarily recalculating
            # view indices
            self._upsert_raw('_design/{}'.format(name), design)

    def define_view(self, design: str, name: str, map_fn: str, reduce_fn: Optional[str] = None):
        '''
        Define a MapReduce view

        @param  design     Design document name
        @param  name       View name
        @param  map_fn     Map JavaScript function
        @param  reduce_fn  Reduce JavaScript function (optional)
        '''
        doc = self._define_design(design)

        # Make sure we have a `views` item
        if 'views' not in doc:
            doc['views'] = {}
    
        # Create/overwrite view
        doc['views'][name] = {'map': map_fn}
        if reduce_fn:
            doc['views'][name]['reduce'] = reduce_fn

    def define_update(self, design: str, name: str, handler_fn: str):
        '''
        Define an update handler

        @param  design      Design document name
        @param  name        Update handler name
        @param  handler_fn  Update handler JavaScript function
        '''
        doc = self._define_design(design)

        # Make sure we have an `updates` item
        if 'updates' not in doc:
            doc['updates'] = {}

        # Create/overwrite update handler
        doc['updates'][name] = handler_fn


class Bert(_Couch):
    ''' Interface to the queue database documents '''
    @staticmethod
    def _reset_processing(query_row: Row) -> Document:
        '''
        Wrapper function to query that converts returned result rows
        into documents with their processing state reset
        '''
        doc = Document(query_row['value'])
        doc['dirty']      = True
        doc['processing'] = False
        doc['queue_from'] = _now()
        
        return doc

    def __init__(self, host: str, database: str):
        '''
        Constructor: Connect to the database and create, wherever
        necessary, the views and update handlers to provide the queue
        management interface

        @param  host      CouchDB host URL
        @param  database  Database name
        '''
        super().__init__()

        self._define_schema()
        self.connect(host, database)

        # If there are any files marked as currently processing, this
        # must be due to a previous failure. Reset all of these for
        # immediate reprocessing
        in_progress = self.query('queue', 'in_progress', Bert._reset_processing, reduce = False)
        if len(in_progress):
            self._db.update(in_progress.rows)

    def _get_id(self, path: str) -> Optional[str]:
        '''
        Get queue document ID by file path
        
        @param  path  File path
        '''
        results = self.query('queue', 'get_id', key = path, reduce = False)
        return results.rows[0].value if len(results) else None

    def queue_length(self) -> int:
        '''
        @return The current (for-processing) queue length
        '''
        results = self.query('queue', 'to_process', endkey = _now(),
                                                    reduce = True,
                                                    group  = False)

        return results.rows[0].value if len(results) else 0

    def mark_dirty(self, path: str, lead_time: timedelta = None):
        '''
        Mark a file as requiring, potentially delayed, (re)processing

        @param  path       File path
        @param  lead_time  Requeue lead time
        '''
        # Get document ID
        doc_id = self._get_id(path)

        if doc_id:
            # Determine queue time
            queue_from = _now()
            if lead_time:
                queue_from += lead_time.total_seconds()

            self.upsert('queue', 'set_state', doc_id, dirty      = True,
                                                      queue_from = queue_from)

        else:
            self.upsert('queue', 'set_state', location = path)

    def dequeue(self) -> Optional[str]:
        '''
        Get the next document on the queue and mark it as processing

        @return File path (None, if empty queue)
        '''
        results = self.query('queue', 'to_process', endkey = _now(),
                                                    reduce = False,
                                                    limit  = 1)
        if len(results):
            latest    = results.rows[0]
            key, path = latest.id, latest.value

            self.upsert('queue', 'set_state',      key, dirty = False)
            self.upsert('queue', 'set_processing', key, processing = True)
            return path
    
    def mark_finished(self, path: str):
        '''
        Mark a file as finished processing

        @param  path  File path
        '''
        # Get document ID
        doc_id = self._get_id(path)

        if doc_id:
            self.upsert('queue', 'set_processing', doc_id, processing = False)

    def _define_schema(self):
        ''' Define views and update handlers '''
        # View: queue/to_process
        # Queue documents marked as dirty and not currently processing
        # Keyed by `queue_from`, set the endkey in queries appropriately
        # Reduce to the number of items in the queue
        self.define_view('queue', 'to_process',
            map_fn = '''
                function(doc) {
                    if (doc.queue && doc.dirty && !doc.processing) {
                        emit(doc.queue_from, doc.location);
                    }
                }
            ''',
            reduce_fn = '_count'
        )

        # View: queue/in_progress
        # Queue documents marked as currently processing
        self.define_view('queue', 'in_progress',
            map_fn = '''
                function(doc) {
                    if (doc.queue && doc.processing) {
                        emit(doc._id, doc)
                    }
                }
            '''
        )

        # View: queue/get_id
        # Queue documents, keyed by their file path
        self.define_view('queue', 'get_id',
            map_fn = '''
                function (doc) {
                    if (doc.queue) {
                        emit(doc.location, doc._id);
                    }
                }
            '''
        )

        # Update handler: queue/set_state
        # Create a new queue document (expects `location` in the query
        # string), or update a queue document's dirty status (expects
        # `dirty` in the query string and an optional `queue_from`)
        self.define_update('queue', 'set_state',
            handler_fn = '''
                function(doc, req) {
                    var q   = req.query || {},
                        now = Math.floor(Date.now() / 1000);

                    // Create new item in queue
                    if (!doc && req.id) {
                        if ('location' in q) {
                            return [{
                                _id:        req.id,
                                location:   q.location,
                                queue:      true,
                                dirty:      true,
                                processing: false,
                                queue_from: now
                            }, 'created'];
                        }
                    }

                    // Update
                    if (doc.queue && 'dirty' in q) {
                        doc.dirty      = (q.dirty == 'true');
                        doc.queue_from = doc.dirty ? parseInt(q.queue_from || now, 10)
                                                   : null;

                        return [doc, 'updated'];
                    }

                    // Failure
                    return [null, 'failed'];
                }
            '''
        )

        # Update handler: queue/set_processing
        # Update a queue document's processing status
        self.define_update('queue', 'set_processing',
            handler_fn = '''
                function(doc, req) {
                    var q = req.query || {};

                    // Update
                    if (doc && doc.queue && 'processing' in q) {
                        doc.processing = (q.processing == 'true');
                        return [doc, 'updated'];
                    }

                    // Failure
                    return [null, 'failed'];
                }
            '''
        )


class Ernie(_Couch):
    ''' Interface to the metadata database documents '''
    def __init__(self, host: str, database: str):
        '''
        Constructor: Connect to the database and create the metadata
        view, if necessary.

        @param  host      CouchDB host URL
        @param  database  Database name
        '''
        super().__init__()

        # Create views
        # self.define_view('metadata', 'resolve',
        #     map_fn = 'function(doc) { emit([doc.path, doc.source], doc._id); }'
        # )

        # self.define_view('metadata', 'aggregate',
        # 


        # self.upsert('_design/metadata', {
        #     'lanugage': 'javascript',
        #     'views': {
        #         # Get document IDs keyed by path and source
        #         'resolve': {
        #             'map': 'function(doc) { emit([doc.path, doc.source], doc._id]); }'
        #         },

        #         # Get the list of (latest) enrichments by path
        #         'aggregate': {
        #             'map': '''function(doc) {
        #                 emit(doc.path, {
        #                     source:    doc.source,
        #                     timestamp: doc.timestamp,
        #                     metadata:  doc.metadata
        #                 });
        #             }''',

        #             # n.b., Exact grouping
        #             'reduce': 'function(keys, values) { return values; }'
        #         }
        #     }
        # })

        self.connect(host, database)
