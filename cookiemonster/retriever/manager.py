import logging
from abc import abstractmethod, ABCMeta
from datetime import datetime, timedelta
from multiprocessing import Lock
from threading import Timer, Thread

import math
from hgicommon.listenable import Listenable

from cookiemonster.retriever._models import QueryResult, RetrievalLog
from cookiemonster.retriever._retriever import FileUpdateRetriever
from cookiemonster.retriever.mappers import RetrievalLogMapper


class RetrievalManager(Listenable, metaclass=ABCMeta):
    """
    Manages the retrieval of file updates.
    """
    def __init__(self, file_update_retriever: FileUpdateRetriever, retrieval_log_mapper: RetrievalLogMapper):
        """
        Default constructor.
        :param retrieval_period: the period that dictates the frequency at which data is retrieved
        :param file_update_retriever: the object through which file updates can be retrieved from the source
        :param retrieval_log_mapper: mapper through which retrieval logs can be stored
        """
        super(RetrievalManager, self).__init__()
        self._file_update_retriever = file_update_retriever
        self._retrieval_log_mapper = retrieval_log_mapper

    @abstractmethod
    def run(self, file_updates_since: datetime=datetime.min):
        """
        Runs the retriever in the same thread.
        :param file_updates_since: the time from which to get file updates from (defaults to getting all updates).
        """
        pass

    def _do_retrieve(self, file_updates_since: datetime) -> QueryResult:
        """
        Handles the retrieval of file updates by getting the data using the retriever, notifying the listeners and then
        logging the retrieval.
        :param file_updates_since: the time from which to retrieve updates since
        :return: the query result that was retrieved
        """
        logging.debug("Starting file update retrieval...")
        query_result = self._file_update_retriever.query_for_all_file_updates_since(file_updates_since)
        assert query_result is not None
        logging.debug("Retrieved %d file updates since %s (query took: %s)"
                      % (len(query_result.file_updates), file_updates_since, query_result.time_taken_to_complete_query))

        if len(query_result.file_updates) > 0:
            logging.debug("Notifying %d listeners of file update" % len(query_result.file_updates))
            self.notify_listeners(query_result.file_updates)

        self._log_retrieval(file_updates_since, query_result)

        return query_result

    def _log_retrieval(self, file_updates_since: datetime, query_result: QueryResult):
        """
        Logs the retrieval of the given query result.
        :param file_updates_since: the time updates were retrieved since
        :param query_result: the query result to log
        """
        retrieval_log = RetrievalLog(file_updates_since, len(query_result.file_updates),
                                     query_result.time_taken_to_complete_query)
        logging.debug("Logging file update query: %s" % retrieval_log)
        self._retrieval_log_mapper.add(retrieval_log)


class PeriodicRetrievalManager(RetrievalManager):
    """
    Manages the periodic retrieval of file updates.
    """
    class _Retrieve:
        """
        TODO
        """
        def __init__(self, file_updates_since: datetime=None, scheduled_for: datetime=None):
            """
            Default constructor.
            :param file_updates_since:
            :param scheduled_for:
            """
            self.file_updates_since = file_updates_since
            self.scheduled_for = scheduled_for

    def __init__(self, retrieval_period: timedelta, file_update_retriever: FileUpdateRetriever,
                 retrieval_log_mapper: RetrievalLogMapper):
        """
        Constructor.
        :param retrieval_period: the period that dictates the frequency at which data is retrieved
        :param file_update_retriever: the object through which file updates can be retrieved from the source
        :param retrieval_log_mapper: mapper through which retrieval logs can be stored
        """
        super(PeriodicRetrievalManager, self).__init__(file_update_retriever, retrieval_log_mapper)
        self._retrieval_period = retrieval_period
        self._timer = None
        self._running = False
        self._started = False
        self._state_lock = Lock()
        self._thread = None

    def start(self, file_updates_since: datetime=datetime.min):
        """
        Starts the periodic retriever in a new thread. Cannot start if already running.
        :param file_updates_since: the time from which to get file updates from (defaults to getting all updates).
        """
        self._state_lock.acquire()
        if self._started or self._running:
            self._state_lock.release()
            raise RuntimeError("Already started")
        self._started = True
        self._state_lock.release()
        Thread(target=self.run, args=(file_updates_since, )).start()

    def run(self, file_updates_since: datetime=datetime.min):
        self._state_lock.acquire()
        if self._running:
            self._state_lock.release()
            raise RuntimeError("Already running")
        self._running = True
        self._state_lock.release()
        retrieve = PeriodicRetrievalManager._Retrieve(file_updates_since, PeriodicRetrievalManager._get_current_time())
        self._do_retrieve_periodically(retrieve)

    def stop(self):
        """
        Stops the periodic retriever.
        """
        self._state_lock.acquire()
        if self._running:
            if self._timer is not None:
                self._timer.cancel()
            self._timer = None
            self._running = False
            self._started = False
            logging.debug("Stopped periodic retrieval manger")
        self._state_lock.release()

    def _do_retrieve_periodically(self, retrieve: _Retrieve):
        """
        Do a retrieve and then schedule next cycle.
        :param retrieve: the retrieve to do
        """
        query_result = self._do_retrieve(retrieve.file_updates_since)

        next_retrieve = PeriodicRetrievalManager._Retrieve()
        next_retrieve.scheduled_for = self._calculate_next_scheduled_time(
            retrieve.scheduled_for, query_result.time_taken_to_complete_query)

        if query_result.time_taken_to_complete_query > self._retrieval_period:
            logging.warning("Query took longer than the period - currently not scheduable!")
        if next_retrieve.scheduled_for >= retrieve.scheduled_for + (2 * self._retrieval_period):
            logging.warning("Query took so long a cycle has been skipped to ensure the period is enforced")

        if len(query_result.file_updates) > 0:
            # Next time, get all updates since the most recent that was received last time
            # XXX: Small delta added to exclude latest result received last time. This makes the assumption that the
            # granularity of the timestamps at the source is the same as here: no smaller than the delta (else records
            # will be missed) and no larger else the exclude will not work and other records could be missed.
            next_retrieve.file_updates_since = query_result.file_updates.get_most_recent()[0].timestamp \
                                          + timedelta.resolution

        if next_retrieve.file_updates_since is None \
                or next_retrieve.file_updates_since < (retrieve.file_updates_since + self._retrieval_period):
            next_retrieve.file_updates_since = retrieve.file_updates_since + self._retrieval_period

        self._schedule_next_retrieve(next_retrieve)

    def _schedule_next_retrieve(self, retrieve: _Retrieve):
        """
        Schedules the next retrieve.
        :param retrieve: model of the scheduled retrieve
        """
        if self._running:
            logging.debug("Scheduling next file update retrieval for: %s" % retrieve.scheduled_for)

            if PeriodicRetrievalManager._get_current_time() >= retrieve.scheduled_for:
                # Next cycle should begin straight away
                self._do_retrieve_periodically(retrieve)
            else:
                interval = retrieve.scheduled_for - PeriodicRetrievalManager._get_current_time()
                # Run timer in same thread
                self._timer = Timer(
                    interval.total_seconds(), self._do_retrieve_periodically, args=(retrieve, ))
                self._timer.run()

    def _calculate_next_scheduled_time(self, previous_scheduled_time: datetime, previous_query_took: timedelta):
        """
        Given the time at which a job was previous scheduled to start at and the computation time of that job,
        calculates when the next period should be scheduled for.

        Periodic drift is avoided by not scheduling based on current time. However, given that the computation time
        (time to complete the query, assuming actual "computation time involved by running the retriever is 0) is
        unbounded, measures have to be taken to guarantee a cycle is never completed early (R_{x+1} >= R_{x} + T)
        particularly in the case where c >= 2T (i.e. retrieval should be "skipped" as it is time for the next
        retrieval).
        :param previous_scheduled_time: the time at which the previous job was scheduled for
        :param previous_query_took: the computation time of the previous job
        """
        c = previous_query_took
        T = self._retrieval_period
        delta = timedelta.resolution
        R_0 = previous_scheduled_time

        R_1 = R_0 + max(1, math.ceil(c / (T - delta)) - 1) * T
        assert R_1 >= R_0 + self._retrieval_period
        return R_1

    @staticmethod
    def _get_current_time() -> datetime:
        """
        Gets the current time. Can be overriden to control environment for testing.
        :return: the current time
        """
        return datetime.now()
