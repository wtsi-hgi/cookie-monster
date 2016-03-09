import logging
from abc import abstractmethod, ABCMeta
from copy import copy
from datetime import datetime, timedelta
from threading import Lock, Timer
from typing import Dict, Union, Optional, Iterable
from typing import List

import atexit

from cookiemonster.logging.models import Log
from cookiemonster.logging.types import RecordableValue


class Logger(metaclass=ABCMeta):
    """
    Records logs.
    """
    def __init__(self, buffer_latency: Optional[timedelta]=None):
        self.buffer_latency = buffer_latency

        self._buffer = []    # type: List[Log]
        self._buffer_timer = None   # type: Optional[Timer]
        self._buffer_lock = Lock()

        # Flush the buffer on exit
        atexit.register(self.flush)

    def record(self, measured: str, values: Union[RecordableValue, Dict[str, RecordableValue]], metadata: Dict=None,
               timestamp: datetime=None):
        """
        Records the given dated measurement value and any metadata.
        :param measured: the name of the variable that has been measured
        :param values: a value or dictionary of named values that describe the measured variable
        :param metadata: any metadata associated to the measurement
        :param timestamp: when the measurement was taken
        """
        log = Log(measured, values, metadata, timestamp)

        with self._buffer_lock:
            self._buffer.append(log)
            if self._buffer_timer is None and self.buffer_latency is not None:
                self._buffer_timer = Timer(self.buffer_latency.total_seconds(), self.flush)
                self._buffer_timer.start()

        if self.buffer_latency is None or self.buffer_latency.total_seconds() == 0:
            self.flush()

    def flush(self):
        """
        Flush any logs from the buffer.
        """
        with self._buffer_lock:
            logs = copy(self._buffer)
            self._buffer.clear()

            if self._buffer_timer is not None and self._buffer_timer.is_alive():
                self._buffer_timer.cancel()
            self._buffer_timer = None

        if len(logs) > 0:
            self._write_logs(logs)

    @abstractmethod
    def _write_logs(self, logs: Iterable[Log]):
        """
        Writes the given logs.
        :param logs: the logs to write
        """


class PythonLoggingLogger(Logger):
    """
    Logger that passes logs to Python's `logging.info`.
    """
    def _write_logs(self, logs: Iterable[Log]):
        for log in logs:
            logging.info("%s: %s = %s (%s)" % (log.timestamp, log.measured, log.values, log.metadata))
