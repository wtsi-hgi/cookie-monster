from datetime import date, datetime, timedelta
from threading import Timer, Thread

from cookiemonster.common.listenable import Listenable
from cookiemonster.retriever._models import QueryResult, RetrievalLog
from cookiemonster.retriever._retriever import FileUpdateRetriever
from cookiemonster.retriever.mappers import RetrievalLogMapper


class RetrievalManager(Listenable):
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
        super(RetrievalManager, self).__init__()
        self._retrieval_period = retrieval_period
        self._file_update_retriever = file_update_retriever
        self._retrieval_log_mapper = retrieval_log_mapper
        self._listeners = []
        self._timer = None
        self._latest_retrieved_timestamp = date.min
        self._running = False

    def start(self, file_updates_since: datetime=date.min):
        """
        Starts the periodic retriever in a new thread. Cannot start if already running.
        :param file_updates_since: the time from which to get file updates from (defaults to getting all updates).
        """
        if self._running:
            raise RuntimeError("Already running")
        self._running = True
        Thread(target=self.run, args=file_updates_since)

    def run(self, file_updates_since: datetime=date.min):
        """
        Runs the periodic retriever in the same thread.
        :param file_updates_since: the time from which to get file updates from (defaults to getting all updates).
        """
        if self._running:
            raise RuntimeError("Already running")
        self._running = True
        self._latest_retrieved_timestamp = file_updates_since
        retrieve_was_scheduled_for = RetrievalManager._get_current_time()
        self._schedule_next_periodic_retrieve(retrieve_was_scheduled_for)

    def stop(self):
        """
        Stops the periodic retriever.
        """
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
            self._running = True

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
        if RetrievalManager._get_current_time() >= retrieve_next_at:
            # Next cycle should begin straight away
            self._do_retrieve_periodically(retrieve_next_at)
        else:
            interval = retrieve_next_at - RetrievalManager._get_current_time()
            # Run timer in same thread
            self._timer = Timer(interval, self._do_retrieve_periodically, retrieve_next_at).run()

    def _do_retrieve(self, file_updates_since: datetime):
        """
        Handles the retrieval of file updates by getting the data using the retriever, notifying the listeners and then
        logging the retrieval.
        :param file_updates_since: the time from which to retrieve updates since
        """
        query_result = self._file_update_retriever.query_for_all_file_updates_since(file_updates_since)

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
        self._retrieval_log_mapper.add(retrieval_log)

    @staticmethod
    def _get_current_time() -> datetime:
        """
        Gets the current time. Can be overriden to control environment for testing.
        :return: the current time
        """
        return datetime.now()
