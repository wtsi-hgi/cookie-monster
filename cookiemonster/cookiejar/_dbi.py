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

_DBI
----
`DBI` is the base CouchDB class used to provide a common interface with
the database instance. It should be the parent of instantiable classes,
which should initialise it with the database host URL and database name.
Upon which, it will acquire a connection and create the database, if it
does not already exist.

Methods:

* `fetch` Get a document by its ID and, optionally, its revision

* `upsert` Insert/update a document into the database by its ID

Bert (Queue Management DBI)
---------------------------
TODO

Schema:

    _id             file path
    state           [ProcessingQueueState]
    queue_from      queue timestamp (Unix time)
    last_processed  null | metadata revision ID

Ernie (Metadata Repository DBI)
-------------------------------
TODO

Schema:

    _id             file path
    [Metadata key-values...]

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

from typing import Optional

import couchdb
from couchdb.client import Document

from hgicommon.collections import Metadata

from cookiemonster.common.enums import ProcessingQueueState


class _DBI(object):
    '''
    Connect (and create, if necessary) to the database on the specified
    host, plus provide an interface to interact with the data
    '''
    def __init__(self, host: str, database: str):
        '''
        Constructor: Acquire connection with the CouchDB host and use
        (and create) the database

        @param  host      CouchDB host URL
        @param  database  Database name
        '''
        self._couch = couchdb.Server(host)
        
        if database not in self._couch:
            self._couch.create(database)

        self._db = self._couch[database]

    def fetch(self, key: str, revision: Optional[str] = None) -> Optional[Document]:
        '''
        Get a database document by its ID and, optionally, revision

        @param  key       Document ID
        @param  revision  Revision ID
        @return Database document (or None, if not found)
        '''
        output = None

        if key in self._db:
            if revision is None:
                output = self._db[key]
            else:
                output = next((doc for doc in self._db.revisions(key) if doc['_rev'] == revision), None)

        return output

    def upsert(self, key: str, document: dict) -> str:
        '''
        Insert or update a document by ID

        @param  key       Document ID
        @param  document  Dictionary
        @return The revision ID for the document

        n.b., If the document hasn't changed since the current revision,
        then no insert is performed and that revision's ID will be
        returned
        '''

        # Check key exists
        #   If it does, fetch it
        #     Check for changes
        #       Yes  update
        #       No   do nothing
        #   If it doesn't, we insert

        # TODO: Update this...
        # doc = document.copy()
        # doc['_id'] = str(doc_key)
        # 
        # if doc_key in self._db:
        #     # Update (if necessary)
        #     current = _document_to_metadata(self._db[doc_key])
        #     if metadata != current:
        #         # To avoid update conflicts, we must explicitly set the
        #         # revision key to the latest
        #         doc['_rev'] = self._db[doc_key]['_rev']
        #         _, _rev = self._db.save(doc)
        #     else:
        #         _rev = self._db[doc_key]['_rev']

        # else:
        #     # Insert
        #     _, _rev = self._db.save(doc)

        # return _rev


class Bert(_DBI):
    '''
    Interface for creating and interacting with the queue database
    '''
    def __init__(self, host: str, database: str):
        '''
        Constructor: Connect to the database and create the processing
        queue view, if necessary. The view shows all non-complete
        documents with a `queue_from` time on or before the current
        host time.

        @param  host      CouchDB host URL
        @param  database  Database name
        '''
        super().__init__(host, database)

        # Create views
        self.upsert('_design/queue', {
            'language': 'javascript',
            'views': {
                'to_process': {
                    'map': '''function(doc) {
                        var now = Math.floor(Date.now() / 1000);

                        if (doc.state != 'complete' && doc.queue_from <= now) {
                            emit(doc.queue_from, {
                                path:           doc._id,
                                last_processed: doc.last_processed
                            });
                        }
                    }''',

                    'reduce': '_count'
                }
            }
        })


class Ernie(_DBI):
    '''
    Interface for creating and interacting with the metadata database
    '''
    def __init__(self, host: str, database: str):
        super().__init__(host, database)
