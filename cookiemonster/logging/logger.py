import logging
from abc import abstractmethod, ABCMeta
from datetime import datetime
from typing import Dict, Union

from cookiemonster.logging.models import Log
from cookiemonster.logging.types import RecordableValue


class Logger(metaclass=ABCMeta):
    """
    Records logs.
    """
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
        logging.info("%s: %s = %s (%s)" % (log.timestamp, log.measured, log.values, log.metadata))
