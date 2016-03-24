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
