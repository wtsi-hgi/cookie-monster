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
from collections import deque, OrderedDict
from copy import deepcopy
from datetime import timedelta
from enum import Enum
from time import monotonic, sleep
from typing import Callable, List, Tuple
from threading import Lock, Thread

from hgicommon.mixable import Listenable


class Actions(Enum):
    ''' Enumeration of database actions '''
    Upsert = 1
    Delete = 2


class _DocumentBuffer(Listenable[List[dict]]):
    ''' Document buffer '''
    def __init__(self, max_size:int, latency:timedelta):
        super().__init__()

        self._max_size = max_size if max_size > 0 else 1
        self._latency = latency.total_seconds()

        self._lock = Lock()
        self.data = []
        self.last_updated = monotonic()

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
                latency = monotonic() - self.last_updated
                if len(self.data) >= self._max_size or latency >= self._latency:
                    self.notify_listeners(deepcopy(self.data))
                    self.data[:] = []
                    self.last_updated = monotonic()

    def _watcher(self):
        ''' Watcher thread for time-based discharging '''
        zzz = self._latency / 2

        while self._thread_running:
            self._discharge()
            sleep(zzz)

    def append(self, document:dict):
        ''' Add a document to the buffer '''
        with self._lock:
            self.data.append(document)
            self.last_updated = monotonic()

        # Force size-based discharging
        self._discharge()


class _QueueItem(object):
    ''' Queue item '''
    def __init__(self, action:Actions, docs:List[dict]):
        self.action = action
        self.docs = docs


BatchListenerT = Tuple[Actions, List[dict]]

class _Queue(Listenable[BatchListenerT]):
    def __init__(self, buffer_max_size:int, buffer_latency:timedelta):
        super().__init__()

        # The queue latency is a function of the buffer size and latency
        # TODO...
        self._latency = buffer_latency.total_seconds()

        self._lock = Lock()
        self._queue = deque()

        # Start the watcher
        self._watcher_thread = Thread(target=self._watcher, daemon=True)
        self._thread_running = True
        self._watcher_thread.start()

    def __del__(self):
        self._thread_running = False

    def _dequeue(self):
        '''
        Push the deduplicated top of the queue to its listeners,
        providing there is something listening
        '''
        to_requeue = None

        with self._lock:
            if len(self._listeners) and len(self._queue):
                top = self._queue.popleft()
                docs_to_dequeue = []
                docs_to_requeue = []

                # Get unique documents (by ID) and find duplicates
                document_ids = OrderedDict()
                for index, doc in enumerate(top.docs):
                    doc_id = doc['_id']

                    if doc_id not in document_ids:
                        document_ids[doc_id] = index
                        docs_to_dequeue.append(doc)

                    else:
                        # Duplicates should be requeued
                        docs_to_requeue.append(doc)

                if len(docs_to_requeue):
                    to_requeue = _QueueItem(top.action, docs_to_requeue)

                # Dequeue the documents to listeners
                self.notify_listeners((top.action, docs_to_dequeue))


        if to_requeue:
            self.requeue(to_requeue.action, to_requeue.docs)

    def _watcher(self):
        ''' Watcher thread for time-based dequeueing '''
        zzz = self._latency

        while self._thread_running:
            self._dequeue()
            sleep(zzz)

    def enqueue(self, action:Actions, docs:List[dict]):
        '''
        Add documents to the queue

        @param   action  Database action
        @param   docs    Documents
        '''
        with self._lock:
            self._queue.append(_QueueItem(action, docs))

        self._dequeue()

    def requeue(self, action:Actions, docs:List[dict]):
        '''
        Add documents to the top of the queue

        @param   action  Database action
        @param   docs    Documents
        '''
        with self._lock:
            self._queue.appendleft(_QueueItem(action, docs))

        self._dequeue()


class Buffer(Listenable[BatchListenerT]):
    ''' Buffer and queueing layer '''
    def __init__(self, max_buffer_size:int = 1000, buffer_latency:timedelta = timedelta(milliseconds=50)):
        super().__init__()

        # Operation queue
        self._queue = _Queue(max_buffer_size, buffer_latency)
        self._queue.add_listener(self.notify_listeners)

        # Upsert buffer
        self._upsert_buffer = _DocumentBuffer(max_buffer_size, buffer_latency)
        self._upsert_buffer.add_listener(lambda docs: self._queue.enqueue(Actions.Upsert, docs))

        # Deletion buffer
        self._deletion_buffer = _DocumentBuffer(max_buffer_size, buffer_latency)
        self._upsert_buffer.add_listener(lambda docs: self._queue.enqueue(Actions.Delete, docs))

    def append(self, doc:dict):
        '''
        Add a document into the upsert buffer

        @param   doc  Document
        '''
        self._upsert_buffer.append(doc)

    def remove(self, doc:dict):
        '''
        Add a document into the deletion buffer

        @param   doc  Document
        '''
        self._deletion_buffer.append(doc)

    def requeue(self, action:Actions, docs:List[dict]):
        '''
        Requeue (at the top) any documents with an appropriate action

        @param   action  Database action
        @param   docs    List of documents
        '''
        self._queue.requeue(action, docs)
