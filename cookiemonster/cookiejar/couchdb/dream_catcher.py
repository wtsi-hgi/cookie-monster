'''
Database Interface
==================
Abstraction layer over a revisionable document-based database (i.e.,
CouchDB), with queue management and metadata repository interfaces

Exportable classes: `Sofabed`, `Bert`, `Ernie`

Sofabed
-------



'''
import json
from collections import deque, OrderedDict
from copy import deepcopy
from datetime import timedelta
from os import environ
from threading import Lock, Thread
from time import monotonic, sleep, time
from typing import Any, Callable, Generator, Iterable, Optional, Tuple
from uuid import uuid4

from hgicommon.mixable import Listenable













class _DocumentBuffer(Listenable):
    ''' Document buffer '''
    def __init__(self, max_size:int = 100, latency:timedelta = timedelta(milliseconds=50)):
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




