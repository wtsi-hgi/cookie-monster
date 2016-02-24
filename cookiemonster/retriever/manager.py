import logging
import time
from datetime import datetime
from multiprocessing import Lock
from threading import Thread
from typing import TypeVar

from apscheduler.schedulers.blocking import BlockingScheduler
from hgicommon.mixable import Listenable

from cookiemonster.common.collections import UpdateCollection
from cookiemonster.common.helpers import localise_to_utc
from cookiemonster.retriever._models import RetrievalLog
from cookiemonster.retriever.mappers import RetrievalLogMapper, UpdateMapper

TimeDeltaInSecondsT = TypeVar("TimeDelta")


class RetrievalManager(Listenable[UpdateCollection]):
    """
    Manages the retrieval of updates.
    """
    def __init__(self, update_mapper: UpdateMapper, retrieval_log_mapper: RetrievalLogMapper):
        """
        Default constructor.
        :param update_mapper: the object through which updates can be retrieved from the source
        :param retrieval_log_mapper: mapper through which retrieval logs can be stored
        """
        super().__init__()
        self.update_mapper = update_mapper
        self._retrieval_log_mapper = retrieval_log_mapper

    def run(self, updates_since: datetime=datetime.min):
        """
        Runs the retriever in the same thread.
        :param updates_since: the time from which to get updates from (defaults to getting all updates).
        """
        updates_since = localise_to_utc(updates_since)
        self._do_retrieval(updates_since)

    def _do_retrieval(self, updates_since: datetime) -> UpdateCollection:
        """
        Handles the retrieval of updates by getting the data using the retriever, notifying the listeners and then
        logging the retrieval.
        :param updates_since: the time from which to retrieve updates since
        :return: the updates retrieved
        """
        logging.debug("Starting update retrieval...")

        # Do retrieve
        started_at_clocktime = datetime.now()
        started_at = RetrievalManager._get_current_time()
        updates = self.update_mapper.get_all_since(updates_since)
        seconds_taken_to_complete_query = RetrievalManager._get_current_time() - started_at
        assert updates is not None
        logging.debug("Retrieved %d updates since %s (query took: %s)"
                      % (len(updates), updates_since, seconds_taken_to_complete_query))

        # Notify listeners of retrieval
        if len(updates) > 0:
            logging.debug("Notifying %d listeners of %d update(s)" % (len(self.get_listeners()), len(updates)))
            self.notify_listeners(updates)

        # Log retrieval
        retrieval_log = RetrievalLog(started_at_clocktime, seconds_taken_to_complete_query, len(updates), updates_since)
        logging.debug("Logging update query: %s" % retrieval_log)
        self._retrieval_log_mapper.add(retrieval_log)

        return updates

    @staticmethod
    def _get_current_time() -> TimeDeltaInSecondsT:
        """
        Gets the time in seconds according to a monotonic time source.
        :return: the current time
        """
        return time.monotonic()


class PeriodicRetrievalManager(RetrievalManager):
    """
    Manages the periodic retrieval of updates.
    """
    def __init__(self, retrieval_period: TimeDeltaInSecondsT, update_mapper: UpdateMapper,
                 retrieval_log_mapper: RetrievalLogMapper):
        """
        Constructor.
        :param retrieval_period: the period that dictates the frequency at which data is retrieved
        :param update_mapper: the object through which updates can be retrieved from the source
        :param retrieval_log_mapper: mapper through which retrieval logs can be stored
        """
        super().__init__(update_mapper, retrieval_log_mapper)
        self._retrieval_period = retrieval_period
        self._running = False
        self._state_lock = Lock()
        self._updates_since = None  # type: datetime

        self._scheduler = BlockingScheduler()
        self._scheduler.add_job(self._do_periodic_retrieval, "interval", seconds=self._retrieval_period, coalesce=True,
                                max_instances=1, next_run_time=datetime.now())

    def run(self, updates_since: datetime=datetime.min):
        self._updates_since = localise_to_utc(updates_since)

        with self._state_lock:
            if self._running:
                raise RuntimeError("Already running")
            self._running = True
        self._scheduler.start()

    def start(self, updates_since: datetime=datetime.min):
        """
        Starts the periodic retriever in a new thread. Cannot start if already running.
        :param updates_since: the time from which to get updates from (defaults to getting all updates).
        """
        Thread(target=self.run, args=(updates_since, )).start()

    def stop(self):
        """
        Stops the periodic retriever.
        """
        with self._state_lock:
            if self._running:
                self._scheduler.shutdown(wait=False)
                self._running = False
                logging.debug("Stopped periodic retrieval manger")

    def _do_periodic_retrieval(self):
        assert self._updates_since is not None
        updates = self._do_retrieval(self._updates_since)

        if len(updates) > 0:
            # Next time, get all updates since the most recent that was received last time
            self._updates_since = updates.get_most_recent()[0].timestamp
        else:
            # Get all updates since same time in future (not going to move since time forward to simplify things - there
            # is no risk of getting duplicates as no updates in range queried previously). Therefore not changing
            # `self._updates_since`.
            pass
