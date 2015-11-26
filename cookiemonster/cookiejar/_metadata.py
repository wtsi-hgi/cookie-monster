'''
Metadata Database Abstraction
=============================
Operations for managing and interacting with the CouchDB-based metadata
database.

Exportable classes: `MetadataDB`

MetadataDB
----------
`MetadataDB` should be instantiated with the CouchDB database name and
the host URL (defaults to `http://localhost:5984`), upon which it will
acquire a connection. (If the database does not exist, it will be
created.)

The metadata from iRODS comes in the form of "AVUs", which are given a
canonical form in the FileUpdate model, thus serialising them into a
JSON document and performing comparisons against the database is
trivial.

As far as the database's structure is concerned, the `_id` will
correspond with that assigned by the workflow database (albeit,
stringified) and CouchDB's revision system will be used productively.

Methods:

* `fetch` Get a metadata dictionary by its ID and, optionally, its
  revision

* `upsert` Insert/update a metadata dictionary into the database by its
  file ID

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
from hgicommon.collections import Metadata


def _document_to_metadata(doc: couchdb.client.Document) -> Metadata:
    '''
    Strip out the CouchDB keys (_id and _rev) from a document and return
    the canonicalised metadata dictionary

    @param  doc  CouchDB document
    @return Metadata dictionary
    '''
    couch_keys = ['_id', '_rev']
    return Metadata({key: value for key, value in doc.items() if key not in couch_keys})

class MetadataDB(object):
    '''
    Connect (and create, if necessary) to the metadata database, plus
    provide an interface to interact with the data
    '''
    def __init__(self, database: str, url: str = 'http://localhost:5984'):
        '''
        Constructor: Acquire connection with the CouchDB host and use
        (and create) the database

        @param  database  Database name
        @param  url       CouchDB host URL
        '''
        self._couch = couchdb.Server(url)
        
        if database not in self._couch:
            self._couch.create(database)

        self._db = self._couch[database]

    def fetch(self, file_id: int, revision: Optional[str] = None) -> Optional[Metadata]:
        '''
        Get a metadata document by its ID and, optionally, revision

        @param  file_id   File ID number
        @param  revision  Revision ID
        @return Metadata dictionary (or None)
        '''
        doc_key = str(file_id)
        output = None

        if doc_key in self._db:
            if revision is None:
                output = self._db[doc_key]
            else:
                output = next((doc for doc in self._db.revisions(doc_key) if doc['_rev'] == revision), None)

        if output:
            output = _document_to_metadata(output)

        return output

    def upsert(self, file_id: int, metadata: Metadata) -> str:
        '''
        Insert or update a metadata document by ID

        @param  file_id   File ID number
        @param  metadata  Metadata dictionary
        @return The revisions key for the document

        n.b., If the metadata hasn't changed since the current revision,
        then no insert is performed and that revision's key will be
        returned
        '''
        doc_key = str(file_id)

        doc = metadata.copy()
        doc['_id'] = str(doc_key)
        
        if doc_key in self._db:
            # Update (if necessary)
            current = _document_to_metadata(self._db[doc_key])
            if metadata != current:
                # To avoid update conflicts, we must explicitly set the
                # revision key to the latest
                doc['_rev'] = self._db[doc_key]['_rev']
                _, _rev = self._db.save(doc)
            else:
                _rev = self._db[doc_key]['_rev']

        else:
            # Insert
            _, _rev = self._db.save(doc)

        return _rev
