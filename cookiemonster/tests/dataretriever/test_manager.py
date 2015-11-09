import unittest
from datetime import timedelta, date, datetime
from unittest.mock import MagicMock

from cookiemonster.common.models import FileUpdateCollection, FileUpdate
from cookiemonster.dataretriever._models import QueryResult, RetrievalLog
from cookiemonster.dataretriever.manager import RetrievalManager
from cookiemonster.tests.dataretriever.stubs import StubFileUpdateRetriever
from cookiemonster.tests.dataretriever.stubs import StubRetrievalLogMapper


class TestRetrievalManager(unittest.TestCase):
    _TIME_DELTA = timedelta.resolution
    _DATE_TIME = datetime.min

    def setUp(self):
        self._retrieval_period = timedelta(seconds=2)
        self._file_update_retriever = StubFileUpdateRetriever()
        self._retrieval_log_mapper = StubRetrievalLogMapper()
        self._retrieval_manager = RetrievalManager(
            self._retrieval_period, self._file_update_retriever, self._retrieval_log_mapper)
        # Disable timer by default
        self._retrieval_manager._set_timer_for_next_periodic_retrieve = MagicMock()

        self._file_updates = FileUpdateCollection()
        query_result = QueryResult(self._file_updates, TestRetrievalManager._TIME_DELTA)
        self._file_update_retriever.query_for_all_file_updates_since = MagicMock(return_value=query_result)

    def test__do_retrieve(self):
        listener = MagicMock()
        self._retrieval_manager.add_listener(listener)
        self._retrieval_manager._do_retrieve(TestRetrievalManager._DATE_TIME)

        self._retrieval_log_mapper.add = MagicMock()

        # Check that retrieves updates from source
        self._file_update_retriever.query_for_all_file_updates_since.assert_called_once_with(
            TestRetrievalManager._DATE_TIME)
        # Check that updates listener
        listener.assert_called_once_with(self._file_updates)
        # Check that retrieval is logged
        # self._retrieval_log_mapper.add.assert_called_once_with(RetrievalLog())
        # latest_retrieved_timestamp: datetime, number_of_file_updates: int, time_taken_to_complete_query: timedelta



    #
    # def test_start_to_do_retrieve_with_no_updates(self):
    #     file_updates = FileUpdateCollection()
    #     query_result = QueryResult(file_updates,  timedelta.resolution)
    #     self._file_update_retriever.query_for_all_file_updates_since = MagicMock(return_value=query_result)
    #     listener = MagicMock()
    #     self._retrieval_manager.add_listener(listener)
    #     self._retrieval_manager.start()
    #
    #     self._file_update_retriever.query_for_all_file_updates_since.assert_called_once_with(date.min)
    #     listener.assert_called_once_with(file_updates)
    #
    # def test_start_to_do_retrieve_with_updates(self):
    #     file_updates = FileUpdateCollection(
    #         [FileUpdate("", "", date.min + timedelta.resolution), FileUpdate("", "", date.max)])
    #     query_result = QueryResult(file_updates,  timedelta.resolution)
    #     self._file_update_retriever.query_for_all_file_updates_since = MagicMock(return_value=query_result)
    #     listener = MagicMock()
    #     self._retrieval_manager.add_listener(listener)
    #     self._retrieval_manager.start()
    #
    #     self._file_update_retriever.query_for_all_file_updates_since.assert_called_once_with(date.min)
    #     listener.assert_called_once_with(file_updates)
    #     self.assertEquals(self._retrieval_manager._latest_retrieved_timestamp, date.max)
    #
    # def test_start_with_file_updates_since(self):
    #     file_updates_since = date.min + timedelta.resolution
    #     file_updates = FileUpdateCollection(
    #         [FileUpdate("", "", date.min), FileUpdate("", "", file_updates_since), FileUpdate("", "", date.max)])
    #     query_result = QueryResult(file_updates[1:],  timedelta.resolution)
    #     self._file_update_retriever.query_for_all_file_updates_since = MagicMock(return_value=query_result)
    #     listener = MagicMock()
    #     self._retrieval_manager.add_listener(listener)
    #     self._retrieval_manager.start(file_updates_since)
    #
    #     self._file_update_retriever.query_for_all_file_updates_since.assert_called_once_with(file_updates_since)
    #     listener.assert_called_once_with(file_updates[1:])
    #     self.assertEquals(self._retrieval_manager._latest_retrieved_timestamp, date.max)
    #
    # def test_start_correctly_schedules_next(self):
    #     # TODO
    #     raise NotImplementedError()


if __name__ == '__main__':
    unittest.main()
