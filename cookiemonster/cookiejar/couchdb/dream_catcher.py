"""
Buffer and Queueing Layer
=========================
Provides buffering of database actions, which -- once the buffer is
considered full -- are flushed into a queue and ultimately passed back
to the database interface for bulk operation

Exportable classes: Buffer, Actions (Enum)
Exportable type aliases: BatchListenerT

Buffer
------
`Buffer` maintains the upsert and deletion buffers and operation queue.
It should be instantiated with the maximum buffer size (document count)
and the buffer latency.

Methods:

* `append` Add a document to the upsert buffer and, ultimately, the
  operation queue

* `remove` Add a document to the deletion buffer and, ultimately, the
  operation queue

* `requeue` Requeue a set of documents into the appropriate queue, at
  the top (used in event of database/transaction failure)

The `Buffer` will ensure document conflicts can't occur automatically,
by enforcing unique IDs per buffer and requeueing any duplicates.

Note that the `Buffer` does not actually perform any operations against
the database. Instead, it implements `Listenable` and, whenever data is
ready to be pushed from its queues, it injects them via the notifier. It
is then the responsibility of the DB interface class to interact with
the database.

The `Buffer` is thread-safe.

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2016 Genome Research Limited
"""
from datetime import timedelta
from enum import Enum
from typing import Tuple, List
from threading import Thread, Lock
from hgicommon.mixable import Listenable


class Actions(Enum):
    ''' Enumeration of database actions '''
    Upsert = 1
    Delete = 2


BatchListenerT = Tuple[Actions, List[dict]]

class Buffer(Listenable[BatchListenerT]):
    ''' In-memory cache and buffering/queueing layer '''
    def __init__(self, max_buffer_size:int      = 1000,
                       buffer_latency:timedelta = timedelta(milliseconds=50)):
        pass

    def append(self, doc:dict):
        '''
        Add a document into the upsert buffer

        @param   doc  Document
        '''
        pass

    def remove(self, doc:dict):
        '''
        Add a document into the deletion buffer

        @param   doc  Document
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
