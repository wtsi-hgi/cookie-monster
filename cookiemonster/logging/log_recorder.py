from abc import abstractmethod
from cookiemonster.logging.models import Log


class LogRecorder:
    """
    Records logs.
    """
    @abstractmethod
    def log(self, log: Log):
        """
        Records the given log.
        :param log: the log to record
        """
