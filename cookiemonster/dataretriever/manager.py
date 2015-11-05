from datetime import date, datetime, MINYEAR, timedelta
from threading import Timer
from typing import List, Callable

from cookiemonster.common.models import FileUpdate, FileUpdateCollection
from cookiemonster.dataretriever._retriever import FileUpdateRetriever, QueryResult


class RetrievalManager:
    """
    TODO.
    """
    def __init__(self, retrieval_period: timedelta, retriever: FileUpdateRetriever, retrieval_log_mapper: RetrievalLogMapper):
        """
        Constructor.
        :param retrieval_period: the period that dictates the frequency at which data is retrieved
        :param retriever: the object through which file updates can be retrieved from the source
        """
        self._retrieval_period = retrieval_period
        self._retriever = retriever
        self._retrieval_log_mapper = retrieval_log_mapper
        self._listeners = []
        self._timer = None

    def start(self, file_updates_since: datetime=date(MINYEAR, 1, 1)):
        """
        TODO
        :param file_updates_since:
        :return:
        """
        retrieve_next_at = datetime.now()
        self._do_retrieve_periodically(file_updates_since, retrieve_next_at)

    def stop(self):
        """
        TODO
        :return:
        """
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def add_update_listener(self, listener: Callable[List[FileUpdate]]):
        """
        Adds a listener to be called with all the file updates when available.
        :param listener: the callable listener
        """
        self._listeners.append(listener)

    def remove_update_listener(self, listener: Callable[List[FileUpdate]]):
        """
        Removes a file update listener.
        :param listener: the listener to remove
        """
        self._listeners.remove(listener)

    def _do_retrieve_periodically(self, file_updates_since: datetime, retrieve_next_at: datetime):
        """
        TODO
        :param file_updates_since:
        :param retrieve_next_at:
        :return:
        """
        query_result = self._do_retrieve(file_updates_since)
        latest_retrieved_timestamp = query_result.file_updates.get_most_recent().timestamp
        retrieve_next_at = retrieve_next_at + self._retrieval_period
        self._timer = Timer(retrieve_next_at, self._do_retrieve_periodically, latest_retrieved_timestamp, retrieve_next_at)

    def _do_retrieve(self, file_updates_since: datetime) -> QueryResult:
        """
        TODO
        :param file_updates_since:
        :return:
        """
        query_result = self._retriever.query_for_all_file_updates_since(file_updates_since)
        self._notify_listeners(query_result.file_updates)
        self._log_retrieval(query_result)

        return query_result

    def _log_retrieval(self, query_result: QueryResult):
        # retrieval_log = RetrievalLog(latest_retrieved_timestamp, number_of_file_updates: int, time_taken_to_complete_query: timedelta)
        #
        # self._retrieval_log_mapper.add
        raise NotImplementedError()

    def _notify_listeners(self, file_updates: FileUpdateCollection):
        """
        TODO
        :param file_updates:
        :return:
        """
        for listener in self._listeners:
            listener(file_updates)
