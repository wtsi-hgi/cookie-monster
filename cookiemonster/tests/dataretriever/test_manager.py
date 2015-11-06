import unittest
from datetime import timedelta, date
from unittest.mock import MagicMock

from cookiemonster.common.models import FileUpdateCollection, FileUpdate
from cookiemonster.dataretriever._models import QueryResult
from cookiemonster.dataretriever.manager import RetrievalManager
from cookiemonster.tests.dataretriever.stubs import StubFileUpdateRetriever
from cookiemonster.tests.dataretriever.stubs import StubRetrievalLogMapper


class TestRetrievalManager(unittest.TestCase):
    def setUp(self):
        self._retrieval_period = timedelta(seconds=2)
        self._file_update_retriever = StubFileUpdateRetriever()
        self._retrieval_log_mapper = StubRetrievalLogMapper()
        self._retrieval_manager = RetrievalManager(
            self._retrieval_period, self._file_update_retriever, self._retrieval_log_mapper)
        # Disable timer by default
        self._retrieval_manager._set_timer_for_next_periodic_retrieve = MagicMock()

    def test_start_to_do_retrieve_with_no_updates(self):
        file_updates = FileUpdateCollection()
        query_result = QueryResult(file_updates,  timedelta.resolution)
        self._file_update_retriever.query_for_all_file_updates_since = MagicMock(return_value=query_result)
        listener = MagicMock()
        self._retrieval_manager.add_listener(listener)
        self._retrieval_manager.start()

        self._file_update_retriever.query_for_all_file_updates_since.assert_called_once_with(date.min)
        listener.assert_called_once_with(file_updates)

    def test_start_to_do_retrieve_with_updates(self):
        file_updates = FileUpdateCollection(
            [FileUpdate("", "", date.min + timedelta.resolution), FileUpdate("", "", date.max)])
        query_result = QueryResult(file_updates,  timedelta.resolution)
        self._file_update_retriever.query_for_all_file_updates_since = MagicMock(return_value=query_result)
        listener = MagicMock()
        self._retrieval_manager.add_listener(listener)
        self._retrieval_manager.start()

        self._file_update_retriever.query_for_all_file_updates_since.assert_called_once_with(date.min)
        listener.assert_called_once_with(file_updates)
        self.assertEquals(self._retrieval_manager._latest_retrieved_timestamp, date.max)

    def test_start_with_file_updates_since(self):
        file_updates_since = date.min + timedelta.resolution
        file_updates = FileUpdateCollection(
            [FileUpdate("", "", date.min), FileUpdate("", "", file_updates_since), FileUpdate("", "", date.max)])
        query_result = QueryResult(file_updates,  timedelta.resolution)
        self._file_update_retriever.query_for_all_file_updates_since = MagicMock(return_value=query_result)
        listener = MagicMock()
        self._retrieval_manager.add_listener(listener)
        self._retrieval_manager.start(file_updates_since)

        self._file_update_retriever.query_for_all_file_updates_since.assert_called_once_with(file_updates_since)
        listener.assert_called_once_with(file_updates[1:])
        self.assertEquals(self._retrieval_manager._latest_retrieved_timestamp, date.max)






if __name__ == '__main__':
    unittest.main()
