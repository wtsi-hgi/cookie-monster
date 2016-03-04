from abc import abstractmethod, ABCMeta
from cookiemonster.logging.models import Log


class LogRecorder(metaclass=ABCMeta):
    """
    Records logs.
    """
    @abstractmethod
    def log(self, log: Log):
        """
        Records the given log.
        :param log: the log to record
        """


class StubLogRecoder(LogRecorder):
    """
    Stub log recorder.
    """
    def log(self, log: Log):
        pass
