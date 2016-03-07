import logging
from abc import abstractmethod, ABCMeta
from datetime import datetime

from typing import Any, Dict

from cookiemonster.logging.models import Log


class Logger(metaclass=ABCMeta):
    """
    Records logs.
    """
    def record(self, measured: str, value: Any, metadata: Dict=None, timestamp: datetime=datetime.now()):
        """
        Records the given dated measurement value and any metadata.
        :param measured: the name of the variable that has been measured
        :param value: the value of the variable
        :param metadata: any metadata associated to the measurement
        :param timestamp: when the measurement was taken
        """
        log = Log(measured, value, metadata, timestamp)
        self._process_log(log)

    @abstractmethod
    def _process_log(self, log: Log):
        """
        Process the given log.
        :param log: the log to process
        """


class PythonLoggingLogger(Logger):
    """
    Logger that passes logs to Python's `logging.info`.
    """
    def _process_log(self, log: Log):
        logging.info(log.timestamp)
