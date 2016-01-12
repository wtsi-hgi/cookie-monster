import logging
from datetime import datetime, timedelta
from math import ceil
from multiprocessing import Lock
from threading import Timer, Thread

from hgicommon.mixable import Listenable

from cookiemonster.common.collections import UpdateCollection
from cookiemonster.common.helpers import localise_to_utc
from cookiemonster.retriever._models import RetrievalLog
from cookiemonster.retriever.mappers import RetrievalLogMapper, UpdateMapper


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
        self._do_retrieve(updates_since)

    def _do_retrieve(self, updates_since: datetime) -> UpdateCollection:
        """
        Handles the retrieval of updates by getting the data using the retriever, notifying the listeners and then
        logging the retrieval.
        :param updates_since: the time from which to retrieve updates since
        :return: the query result that was retrieved
        """
        logging.debug("Starting update retrieval...")

        # Do retrieve
        started_at = RetrievalManager._get_current_time()
        updates = self.update_mapper.get_all_since(updates_since)
        time_taken_to_complete_query = RetrievalManager._get_current_time() - started_at
        assert updates is not None
        logging.debug("Retrieved %d updates since %s (query took: %s)"
                      % (len(updates), updates_since, time_taken_to_complete_query))

        # Notify listeners of retrieval
        if len(updates) > 0:
            logging.debug("Notifying %d listeners of %d update(s)" % (len(self.get_listeners()), len(updates)))
            self.notify_listeners(updates)

        # Log retrieval
        retrieval_log = RetrievalLog(updates_since, len(updates), time_taken_to_complete_query)
        logging.debug("Logging update query: %s" % retrieval_log)
        self._retrieval_log_mapper.add(retrieval_log)

        return updates

    @staticmethod
    def _get_current_time() -> datetime:
        """
        Gets the current time. Can be overriden to control environment for testing.
        :return: the current time
        """
        return localise_to_utc(datetime.now())


class PeriodicRetrievalManager(RetrievalManager):
    """
    Manages the periodic retrieval of updates.
    """
    class _Retrieve:
        """
        Model of a retrieve.
        """
        def __init__(self, updates_since: datetime=None, scheduled_for: datetime=None):
            """
            Constructor.
            :param updates_since: when to retrieve updates since
            :param scheduled_for: when the retrieval was scheduled for
            """
            self.updates_since = updates_since
            self.scheduled_for = scheduled_for
            self.computation_time = None

    def __init__(self, retrieval_period: timedelta, update_mapper: UpdateMapper,
                 retrieval_log_mapper: RetrievalLogMapper):
        """
        Constructor.
        :param retrieval_period: the period that dictates the frequency at which data is retrieved
        :param update_mapper: the object through which updates can be retrieved from the source
        :param retrieval_log_mapper: mapper through which retrieval logs can be stored
        """
        super().__init__(update_mapper, retrieval_log_mapper)
        self._retrieval_period = retrieval_period
        self._timer = None
        self._running = False
        self._state_lock = Lock()
        self._thread = None

    def run(self, updates_since: datetime=datetime.min):
        updates_since = localise_to_utc(updates_since)

        with self._state_lock:
            if self._running:
                raise RuntimeError("Already running")
            self._running = True
        retrieve = PeriodicRetrievalManager._Retrieve(updates_since, RetrievalManager._get_current_time())
        self._do_retrieve_periodically(retrieve)

    def start(self, updates_since: datetime=datetime.min):
        """
        Starts the periodic retriever in a new thread. Cannot start if already running.
        :param updates_since: the time from which to get updates from (defaults to getting all updates).
        """
        Thread(target=self.run, args=(updates_since,)).start()

    def stop(self):
        """
        Stops the periodic retriever.
        """
        with self._state_lock:
            if self._running:
                if self._timer is not None:
                    self._timer.cancel()
                self._timer = None
                self._running = False
                logging.debug("Stopped periodic retrieval manger")

    def _do_retrieve_periodically(self, retrieve: _Retrieve):
        """
        Do a retrieve and then schedule next cycle.
        :param retrieve: the retrieve to do
        """
        logging.debug("Doing retrieval scheduled for %s" % retrieve.scheduled_for)

        started_computation_at = RetrievalManager._get_current_time()
        updates = self._do_retrieve(retrieve.updates_since)
        computation_time = RetrievalManager._get_current_time() - started_computation_at

        next_retrieve = PeriodicRetrievalManager._Retrieve()
        next_retrieve.scheduled_for = self._calculate_next_scheduled_time(retrieve.scheduled_for, computation_time)

        if computation_time > self._retrieval_period:
            logging.warning("Query took longer than the period - currently not scheduable!")
        if next_retrieve.scheduled_for >= retrieve.scheduled_for + (2 * self._retrieval_period):
            logging.warning("Query took so long a cycle has been skipped to ensure the period is enforced")

        if len(updates) > 0:
            # Next time, get all updates since the most recent that was received last time
            # XXX: Small delta added to exclude latest result received last time. This makes the assumption that the
            # granularity of the timestamps at the source is the same as here: no smaller than the delta (else records
            # will be missed) and no larger else the exclude will not work and other records could be missed.
             next_retrieve.updates_since = localise_to_utc(updates.get_most_recent()[0].timestamp) \
                                           + timedelta.resolution

        if next_retrieve.updates_since is None \
                or next_retrieve.updates_since < (retrieve.updates_since + self._retrieval_period):
            next_retrieve.updates_since = retrieve.updates_since + self._retrieval_period

        self._schedule_next_retrieve(next_retrieve)

    def _schedule_next_retrieve(self, retrieve: _Retrieve):
        """
        Schedules the next retrieve.
        :param retrieve: model of the scheduled retrieve
        """
        if self._running:
            logging.debug("Scheduling next file update retrieval for: %s" % retrieve.scheduled_for)

            if RetrievalManager._get_current_time() >= retrieve.scheduled_for:
                # Next cycle should begin straight away
                self._do_retrieve_periodically(retrieve)
            else:
                interval = retrieve.scheduled_for - RetrievalManager._get_current_time()
                # Run timer in same thread
                self._timer = Timer(interval.total_seconds(), self._do_retrieve_periodically, args=(retrieve, ))
                self._timer.run()

    def _calculate_next_scheduled_time(self, previous_scheduled_time: datetime, previous_computation_time: timedelta):
        """
        Given the time at which a job was previous scheduled to start at and the computation time of that job,
        calculates when the next period should be scheduled for.

        Periodic drift is avoided by not scheduling based on current time. However, given that the computation time
        is unbounded (the query could take a very long time!), measures have to be taken to guarantee a cycle is never
        completed early (R_{x+1} >= R_{x} + SourceDataType) particularly in the case where c >= 2T (i.e. retrieval
        should be "skipped" as it is time for the next retrieval).
        :param previous_scheduled_time: the time at which the previous job was scheduled for
        :param previous_computation_time: the computation time of the previous job
        """
        c = previous_computation_time
        T = self._retrieval_period
        delta = timedelta.resolution
        R_0 = previous_scheduled_time

        R_1 = R_0 + max(1, ceil(c / (T - delta)) - 1) * T
        assert R_1 >= R_0 + self._retrieval_period
        return R_1
