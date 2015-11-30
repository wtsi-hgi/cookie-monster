import logging
from abc import abstractmethod, ABCMeta
from datetime import datetime, timedelta
from threading import Timer, Thread

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
        self._latest_retrieved_timestamp = datetime.min

    @abstractmethod
    def run(self, file_updates_since: datetime=datetime.min):
        """
        Runs the retriever in the same thread.
        :param file_updates_since: the time from which to get file updates from (defaults to getting all updates).
        """
        pass

    def _do_retrieve(self, file_updates_since: datetime):
        """
        Handles the retrieval of file updates by getting the data using the retriever, notifying the listeners and then
        logging the retrieval.
        :param file_updates_since: the time from which to retrieve updates since
        """
        logging.debug("Starting file update retrieval...")
        query_result = self._file_update_retriever.query_for_all_file_updates_since(file_updates_since)
        assert query_result is not None
        logging.debug("Retrieved %d file updates (query took: %s)"
                      % (len(query_result.file_updates), query_result.time_taken_to_complete_query))

        if len(query_result.file_updates) > 0:
            self._latest_retrieved_timestamp = query_result.file_updates.get_most_recent()[0].timestamp

        if len(query_result.file_updates) > 0:
            self.notify_listeners(query_result.file_updates)

        self._log_retrieval(query_result)

    def _log_retrieval(self, query_result: QueryResult):
        """
        Logs the retrieval of the given query result.
        :param query_result: the query result to log
        """
        retrieval_log = RetrievalLog(self._latest_retrieved_timestamp, len(query_result.file_updates),
                                     query_result.time_taken_to_complete_query)
        logging.debug("Logging file update query: %s" % retrieval_log)
        self._retrieval_log_mapper.add(retrieval_log)


class PeriodicRetrievalManager(RetrievalManager):
    """
    Manages the periodic retrieval of file updates.
    """
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

    def start(self, file_updates_since: datetime=datetime.min):
        """
        Starts the periodic retriever in a new thread. Cannot start if already running.
        :param file_updates_since: the time from which to get file updates from (defaults to getting all updates).
        """
        # FIXME: single start logic
        if self._started:
            raise RuntimeError("Already started")
        self._started = True
        Thread(target=self.run, args=(file_updates_since, )).start()

    def run(self, file_updates_since: datetime=datetime.min):
        if self._running:
            raise RuntimeError("Already running")
        self._running = True
        logging.debug("Running periodic retrieval manger")
        self._latest_retrieved_timestamp = file_updates_since
        retrieve_was_scheduled_for = PeriodicRetrievalManager._get_current_time()
        self._schedule_next_periodic_retrieve(retrieve_was_scheduled_for)

    def stop(self):
        """
        Stops the periodic retriever.
        """
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
            self._running = False
            self._started = False
            logging.debug("Stopped periodic retrieval manger")

    def _do_retrieve_periodically(self, retrieval_was_scheduled_for: datetime):
        """
        Do a retrieve and then schedule next cycle.
        :param retrieval_was_scheduled_for: the time at which the retrieval was scheduled for. Required to prevent
        periodic drift
        """
        self._do_retrieve(self._latest_retrieved_timestamp)
        retrieve_next_at = retrieval_was_scheduled_for + self._retrieval_period
        self._schedule_next_periodic_retrieve(retrieve_next_at)

    def _schedule_next_periodic_retrieve(self, retrieve_next_at: datetime):
        """
        Schedules the next cycle.
        :param retrieve_next_at: when the cycle is scheduled for
        """
        logging.debug("Scheduling next file update retrieval for: %s" % retrieve_next_at)

        if PeriodicRetrievalManager._get_current_time() >= retrieve_next_at:
            # Next cycle should begin straight away
            self._do_retrieve_periodically(retrieve_next_at)
        else:
            interval = retrieve_next_at - PeriodicRetrievalManager._get_current_time()
            # Run timer in same thread
            self._timer = Timer(
                interval.total_seconds(), self._do_retrieve_periodically, args=(retrieve_next_at, )).run()

    @staticmethod
    def _get_current_time() -> datetime:
        """
        Gets the current time. Can be overriden to control environment for testing.
        :return: the current time
        """
        return datetime.now()
