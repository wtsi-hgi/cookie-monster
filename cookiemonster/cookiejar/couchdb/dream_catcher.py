"""
In-Memory Cache and Buffering
=============================
Provides an in-memory cache of the most recent actions against the
database and also an action buffering and queueing system to facilitate
bulk operations

Exportable classes: Cache, Actions (Enum)
Exportable exceptions: NotCached
Exportable type aliases: BatchListenerT

Cache
-----
`Cache` maintains an in-memory cache of upserts and deletes and queues
them before pushing them to persistence. This gives us buffering, to
save overloading the database server, with zero-latency for downstream
applications.

The `Cache` should be instantiated with the maximum buffer size (count
of documents), the buffer latency and the maximum cache size (bytes).

Methods:

* `fetch` Get a document from the in-memory cache

* `upsert` Update or insert a document in to the in-memory cache and
  ultimately, via the upsert buffer and queue, the database

* `delete` Mark a document as deleted in the in-memory cache and
  ultimately, via the deletion buffer and queue, the databse

* `requeue` Requeue a set of documents into the appropriate queue, at
  the top (used in event of database/transaction failure)

Note that the `Cache` does not actually perform any operations against
the database. Instead, it implements `Listenable` and, whenever data is
ready to be pushed from its queues, it injects them via the notifier. It
is then the responsibility of the DB interface class to interact with
the database.

The `Cache` is thread-safe.

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2016 Genome Research Limited
"""
from enum import Enum
from typing import Tuple, List, Optional
from threading import Thread, Lock
from hgicommon.mixable import Listenable


class Actions(Enum):
    ''' Enumeration of database actions '''
    Upsert = 1
    Delete = 2


class NotCached(Exception):
    ''' Document state doesn't exist in the cache '''
    pass


BatchListenerT = Tuple[Actions, List[dict]]

class Cache(Listenable[BatchListenerT]):
    ''' In-memory cache and buffering/queueing layer '''
    def __init__(self, max_buffer_size:int      = 100,
                       buffer_latency:timedelta = timedelta(milliseconds=50),
                       max_cache_size:int       = 2097152):
        pass

    def fetch(self, key:str, revision:Optional[str] = None) -> Optional[dict]:
        '''
        Get the document from the cache by its key and, optionally,
        revision IDs

        @param   key       Document ID
        @param   revision  Document revision ID
        @return  The document (None, if not found)

        NOTE If the cache has no record of the document, then a
        NotCached exception will be raised
        '''
        pass

    def upsert(self, doc:dict):
        '''
        Update or insert a document into the in-memory cache and prepare
        it for batch writing to the database

        @param   doc  Document
        '''
        pass

    def delete(self, key:str):
        '''
        Mark a document as deleted in the in-memory cache, if it exists,
        and prepare it for batch deletion from the database

        @param   key  Document ID
        '''
        pass

    def requeue(self, action:Action, docs:List[dict]):
        '''
        Requeue (at the top) any documents with an appropriate action

        @param   action  Database action
        @param   docs    List of documents
        '''
        pass


## from collections import deque, OrderedDict
## from copy import deepcopy
## from datetime import timedelta
## from os import environ
## from threading import Lock, Thread
## from time import monotonic, sleep, time
## from typing import Any, Callable, Generator, Iterable, Optional, Tuple
## from uuid import uuid4
## from enum import Enum
## 
## from hgicommon.mixable import Listenable
## 
## class _DocumentBuffer(Listenable):
##     ''' Document buffer '''
##     def __init__(self, max_size:int = 100, latency:timedelta = timedelta(milliseconds=50)):
##         super().__init__()
## 
##         self._max_size = max_size if max_size > 0 else 1
##         self._latency = latency.total_seconds()
## 
##         self._lock = Lock()
##         self.data = []
##         self.last_updated = monotonic()
## 
##         # Start the watcher
##         self._watcher_thread = Thread(target=self._watcher, daemon=True)
##         self._thread_running = True
##         self._watcher_thread.start()
## 
##     def __del__(self):
##         self._thread_running = False
## 
##     def _discharge(self):
##         '''
##         Discharge the buffer (by pushing the data to listeners) when its
##         state requires so; i.e., when there is something listening and
##         either the buffer size or latency is exceeded
##         '''
##         with self._lock:
##             if len(self._listeners) and len(self.data):
##                 latency = monotonic() - self.last_updated
##                 if len(self.data) >= self._max_size or latency >= self._latency:
##                     self.notify_listeners(deepcopy(self.data))
##                     self.data[:] = []
##                     self.last_updated = monotonic()
## 
##     def _watcher(self):
##         ''' Watcher thread for time-based discharging '''
##         zzz = self._latency / 2
## 
##         while self._thread_running:
##             self._discharge()
##             sleep(zzz)
## 
##     def append(self, document:dict):
##         ''' Add a document to the buffer '''
##         with self._lock:
##             self.data.append(document)
##             self.last_updated = monotonic()
## 
##         # Force size-based discharging
##         self._discharge()
