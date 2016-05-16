"""
Legalese
--------
Copyright (c) 2016 Genome Research Ltd.

Author: Colin Nolan <cn13@sanger.ac.uk>

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
import logging
from abc import abstractmethod, ABCMeta
from copy import copy
from datetime import datetime, timedelta
from threading import Lock, Timer, Thread
from typing import Dict, Union, Optional, Iterable
from typing import List

import atexit

from cookiemonster.logging.models import Log
from cookiemonster.logging.types import RecordableValue


class Logger(metaclass=ABCMeta):
    """
    Records logs.
    """
    @abstractmethod
    def _write_logs(self, logs: Iterable[Log]):
        """
        Writes the given logs.
        :param logs: the logs to write
        """

    def __init__(self, buffer_latency: Optional[timedelta]=None):
        self.buffer_latency_in_seconds = buffer_latency.total_seconds() if buffer_latency is not None else 0
        self._buffer = []    # type: List[Log]
        self._buffer_lock = Lock()
        self._buffer_timer = None   # type: Optional[Timer]

        # Flush the buffer on exit
        atexit.register(self.flush)

    def record(self, measured: str, values: Union[RecordableValue, Dict[str, RecordableValue]], metadata: Dict=None,
               timestamp: datetime=None):
        """
        Records the given dated measurement value(s) and any metadata.

        Non-blocking, thread-safe.
        :param measured: the name of the variable that has been measured
        :param values: a value or dictionary of named values that describe the measured variable
        :param metadata: any metadata associated to the measurement
        :param timestamp: when the measurement was taken. Defaults to current time
        """
        log = Log(measured, values, metadata, timestamp)
        Thread(target=self._blocking_record(log)).start()

    def flush(self):
        """
        Flush any logs from the buffer.

        Thread-safe.
        """
        with self._buffer_lock:
            logs = copy(self._buffer)
            self._buffer.clear()

            if self._buffer_timer is not None and self._buffer_timer.is_alive():
                self._buffer_timer.cancel()
            self._buffer_timer = None

        if len(logs) > 0:
            self._write_logs(logs)

    def _blocking_record(self, log: Log):
        """
        Records the given log.
        :param log: the log to record
        """
        with self._buffer_lock:
            self._buffer.append(log)
            if self._buffer_timer is None and self.buffer_latency_in_seconds > 0:
                self._buffer_timer = Timer(self.buffer_latency_in_seconds, self.flush)
                self._buffer_timer.start()

        if self.buffer_latency_in_seconds == 0:
            self.flush()


class PythonLoggingLogger(Logger):
    """
    Logger that passes logs to Python's `logging.info`.
    """
    def _write_logs(self, logs: Iterable[Log]):
        for log in logs:
            logging.info("%s: %s = %s (%s)" % (log.timestamp, log.measured, log.values, log.metadata))
