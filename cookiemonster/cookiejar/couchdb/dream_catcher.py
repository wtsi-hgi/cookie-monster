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

Legalese
--------
Copyright (c) 2016 Genome Research Ltd.

Author: Christopher Harrison <ch12@sanger.ac.uk>

This file is part of Cookie Monster.

Cookie Monster is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by the
Free Software Foundation; either version 3 of the License, or (at your
option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License along
with this program. If not, see <http://www.gnu.org/licenses/>.
"""
from collections import deque, OrderedDict
from copy import deepcopy
from datetime import timedelta
from enum import Enum
from functools import partial
from threading import Lock, Thread
from time import monotonic, sleep
from typing import Callable, Iterable, List, Tuple, TypeVar

from hgicommon.mixable import Listenable


class Actions(Enum):
    """ Enumeration of database actions """
    Upsert = 1
    Delete = 2


_DischargerT = TypeVar('DischargerT')

class _Discharger(Listenable[_DischargerT]):
    """ Time-based notifier """
    def __init__(self, payload_factory:Callable[..., Iterable], latency:timedelta):
        super().__init__()

        self._discharge_latency = latency.total_seconds()

        self._lock = Lock()
        self._payload = payload_factory()

        # Start the watcher
        self._watcher_thread = Thread(target=self._watcher, daemon=True)
        self._watching = True
        self._watcher_thread.start()

    def __del__(self):
        """ Make the thread exit on garbage collection """
        self._watching = False

    def _watcher(self):
        """ Watcher thread for time-based discharging """
        zzz = self._discharge_latency

        while self._watching:
            self._discharge()
            sleep(zzz)


class _DocumentBuffer(_Discharger[List[dict]]):
    """ Document buffer """
    def __init__(self, max_size:int, latency:timedelta):
        self._max_size = max_size if max_size > 0 else 1
        self._latency = latency.total_seconds()

        super().__init__(list, latency / 2)
        self.last_updated = monotonic()

    def _discharge(self):
        """
        Discharge the buffer (by pushing the data to listeners) when its
        state requires so; i.e., when there is something listening and
        either the buffer size or latency is exceeded
        """
        with self._lock:
            if len(self._listeners) and len(self._payload):
                latency = monotonic() - self.last_updated
                if len(self._payload) >= self._max_size or latency >= self._latency:
                    self.notify_listeners(deepcopy(self._payload))
                    self._payload[:] = []
                    self.last_updated = monotonic()

    def append(self, document:dict):
        """ Add a document to the buffer """
        with self._lock:
            self._payload.append(document)
            self.last_updated = monotonic()

        # Force size-based discharging
        self._discharge()


class _QueueItem(object):
    """ Queue item """
    def __init__(self, action:Actions, docs:List[dict]):
        self.action = action
        self.docs = docs


BatchListenerT = Tuple[Actions, List[dict]]

class _Queue(_Discharger[BatchListenerT]):
    def __init__(self, buffer_latency:timedelta):
        super().__init__(deque, buffer_latency * 2)

    def _discharge(self):
        """
        Push the deduplicated top of the queue to its listeners,
        providing there is something listening
        """
        to_requeue = None

        with self._lock:
            if len(self._listeners) and len(self._payload):
                top = self._payload.popleft()
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

    def enqueue(self, action:Actions, docs:List[dict]):
        """
        Add documents to the queue

        @param   action  Database action
        @param   docs    Documents
        """
        with self._lock:
            self._payload.append(_QueueItem(action, docs))

        self._discharge()

    def requeue(self, action:Actions, docs:List[dict]):
        """
        Add documents to the top of the queue

        @param   action  Database action
        @param   docs    Documents
        """
        with self._lock:
            self._payload.appendleft(_QueueItem(action, docs))

        self._discharge()


class Buffer(Listenable[BatchListenerT]):
    """ Buffer and queueing layer """
    def __init__(self, max_buffer_size:int = 1000, buffer_latency:timedelta = timedelta(milliseconds=50)):
        super().__init__()

        # Operation queue
        self._queue = _Queue(buffer_latency)
        self._queue.add_listener(self.notify_listeners)

        # Action buffers
        self._buffers = {}
        for action in Actions:
            self._buffers[action] = _DocumentBuffer(max_buffer_size, buffer_latency)
            self._buffers[action].add_listener(partial(self._queue.enqueue, action))

    def _buffer(self, action:Actions, doc:dict):
        """
        Add a document to an action buffer

        @param   action  Database action
        @param   doc     Document
        """
        self._buffers[action].append(doc)

    def append(self, doc:dict):
        """ Add a document into the upsert buffer """
        self._buffer(Actions.Upsert, doc)

    def remove(self, doc:dict):
        """ Add a document into the deletion buffer """
        self._buffer(Actions.Delete, doc)

    def requeue(self, action:Actions, docs:List[dict]):
        """
        Requeue (at the top) any documents with an appropriate action

        @param   action  Database action
        @param   docs    List of documents
        """
        self._queue.requeue(action, docs)
