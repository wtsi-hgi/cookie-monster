'''
Cookie Jar
==========
An implementation of `CookieJar` using CouchDB as its database.

Exportable Classes: `BiscuitTin`

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

from cookiemonster.common.models import FileUpdate, CookieCrumbs, CookieProcessState
from cookiemonster.cookiejar._cookiejar import CookieJar
from cookiemonster.cookiejar._dbi import DBI

class BiscuitTin(CookieJar):
    def __init__(self, db_host: str, db_prefix: str):
        pass

    def __call__(self, file_update: FileUpdate):
        pass

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
