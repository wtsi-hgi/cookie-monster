from datetime import date, datetime, timedelta
from threading import Timer

from cookiemonster.common.listenable import Listenable
from cookiemonster.dataretriever._models import QueryResult
from cookiemonster.dataretriever._retriever import FileUpdateRetriever
from cookiemonster.dataretriever.mappers import RetrievalLogMapper


class RetrievalManager(Listenable):
    """
    TODO.
    """
    def __init__(self, retrieval_period: timedelta, file_update_retriever: FileUpdateRetriever,
                 retrieval_log_mapper: RetrievalLogMapper):
        """
        Constructor.
        :param retrieval_period: the period that dictates the frequency at which data is retrieved
        :param file_update_retriever: the object through which file updates can be retrieved from the source
        :param retrieval_log_mapper: TODO
        """
        super(RetrievalManager, self).__init__()
        self._retrieval_period = retrieval_period
        self._file_update_retriever = file_update_retriever
        self._retrieval_log_mapper = retrieval_log_mapper
        self._listeners = []
        self._timer = None
        self._latest_retrieved_timestamp = date.min

    def start(self, file_updates_since: datetime=date.min):
        """
        TODO
        :param file_updates_since:
        :return:
        """
        self._latest_retrieved_timestamp = file_updates_since
        retrieve_was_scheduled_for = RetrievalManager._get_current_time()
        self._do_retrieve_periodically(retrieve_was_scheduled_for)

    def stop(self):
        """
        TODO
        :return:
        """
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def _do_retrieve_periodically(self, retrieve_was_scheduled_for: datetime):
        """
        TODO
        :param file_updates_since:
        :param retrieve_was_scheduled_for:
        :return:
        """
        query_result = self._do_retrieve(self._latest_retrieved_timestamp)
        retrieve_next_at = retrieve_was_scheduled_for + self._retrieval_period
        if len(query_result.file_updates) > 0:
            self._latest_retrieved_timestamp = query_result.file_updates.get_most_recent()[0].timestamp
        self._set_timer_for_next_periodic_retrieve(retrieve_next_at)

    def _set_timer_for_next_periodic_retrieve(self, retrieve_next_at: datetime):
        """
        TODO
        :param retrieve_next_at:
        :param file_updates_since:
        :return:
        """
        self._timer = Timer(retrieve_next_at, self._do_retrieve_periodically, retrieve_next_at)

    def _do_retrieve(self, file_updates_since: datetime) -> QueryResult:
        """
        TODO
        :param file_updates_since:
        :return:
        """
        query_result = self._file_update_retriever.query_for_all_file_updates_since(file_updates_since)
        self.notify_listeners(query_result.file_updates)
        self._log_retrieval(query_result)
        return query_result

    def _log_retrieval(self, query_result: QueryResult):
        # retrieval_log = RetrievalLog(latest_retrieved_timestamp, number_of_file_updates: int, time_taken_to_complete_query: timedelta)
        #
        # self._retrieval_log_mapper.add
        pass

    @staticmethod
    def _get_current_time() -> datetime:
        """
        Gets the current time. Can be overriden to control environment for testing.
        :return: the current time
        """
        return datetime.now()