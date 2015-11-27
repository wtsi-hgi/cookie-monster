'''
Cookie Jar
==========
An implementation of `CookieJar` using CouchDB as its database.

Exportable Classes: `BiscuitTin`, `CookieJar` (by proxy)

BiscuitTin
----------
`BiscuitTin` implements the interface decreed by `CookieJar`. It must be
instantiated with the CouchDB host URL and database name prefix. It will
connect to (and create, wherever necessary) two databases on the
provided host: `{prefix}-queue` and `{prefix}-metadata`, which will
manage the processing queue and metadata for files (using their path as
document IDs), respectively.

In additional, instantiations are callable as a proxy to
`enrich_metadata`, with the caveat of data passed as `FileUpdate`
models, which will be appropriately munged into `CookieCrumbs`. The
intention is that this proxy will serve as the listener to the data
retriever.

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015 Genome Research Limited
'''

from datetime import timedelta
from typing import Optional

from cookiemonster.common.enums import MetadataNS
from cookiemonster.common.models import FileUpdate, CookieCrumbs, CookieProcessState
from cookiemonster.cookiejar._cookiejar import CookieJar
from cookiemonster.cookiejar._dbi import DBI

class BiscuitTin(CookieJar):
    def __init__(self, db_host: str, db_prefix: str):
        '''
        Constructor: Initialise the database connections

        @param  db_host    Database host URL
        @param  db_prefix  Database name prefix
        '''
        self._queue    = DBI(db_host, '{}-queue'.format(db_prefix))
        self._metadata = DBI(db_host, '{}-metadata'.format(db_prefix))

    def __call__(self, file_update: FileUpdate):
        '''
        Proxy to enrich_metadata, taking a FileUpdate model as input and
        converting it appropriately

        @param  file_update  A FileUpdate model from upstream
        '''
        path = file_update.file_location
        metadata = CookieCrumbs()

        # Convert IRODS metadata into CookieCrumbs
        metadata.set(MetadataNS.IRODS.FileSystem, 'hash', file_update.file_hash)
        metadata.set(MetadataNS.IRODS.FileSystem, 'timestamp', file_update.timestamp)
        for key, value in file_update.metadata.items():
            metadata.set(MetadataNS.IRODS.AVUs, key, value)

        self.enrich_metadata(path, metadata)

    def enrich_metadata(self, path: str, metadata: CookieCrumbs):
        pass

    def mark_as_failed(self, path: str, requeue_delay: timedelta):
        pass

    def mark_as_complete(self, path: str):
        pass

    def mark_as_reprocess(self, path: str):
        pass

    def get_next_for_processing(self) -> Optional[CookieProcessState]:
        pass

    def queue_length(self) -> int:
        pass
