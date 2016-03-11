from abc import ABCMeta, abstractmethod

from datetime import timedelta, datetime
from threading import Lock

from apscheduler.job import Job
from apscheduler.schedulers.background import BackgroundScheduler

from cookiemonster.logging.logger import Logger


class Monitor(metaclass=ABCMeta):
    """
    TODO
    """
    _SCHEDULER = BackgroundScheduler()
    _SCHEDULER.start()

    def __init__(self, logger: Logger, period: timedelta):
        """
        Constructor.
        :param logger: logger to use to record logs
        :param period: how often the monitor should record a log
        """
        self._logger = logger
        self._period = period
        self._job = None    # type: Job
        self._status_lock = Lock()

    def is_running(self) -> bool:
        """
        Gets whether the monitor is running.
        :return: whether the monitor is running
        """
        return self._job is not None

    def start(self):
        """
        Starts the monitor. Has no effect if has been already started.
        """
        with self._status_lock:
            if not self.is_running():
                self._job = Monitor._SCHEDULER.add_job(self.do_log_record, "interval", next_run_time=datetime.now(),
                                                       seconds=self._period.total_seconds())

    def stop(self):
        """
        Stops the monitor. Has no effect if not started.
        """
        with self._status_lock:
            if self.is_running():
                self._job.remove()

    @abstractmethod
    def do_log_record(self):
        """
        TODO
        """
