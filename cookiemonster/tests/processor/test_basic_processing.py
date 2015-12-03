import unittest
from datetime import datetime
from multiprocessing import Lock
from threading import Thread
from typing import Iterable, Callable, List
from unittest.mock import MagicMock, call

from multiprocessing import Semaphore

from hgicommon.collections import Metadata

from cookiemonster.common.models import Cookie, Notification, Enrichment
from cookiemonster.processor._enrichment import EnrichmentManager
from cookiemonster.processor._models import Rule, EnrichmentLoader
from cookiemonster.processor._models import RuleAction
from cookiemonster.processor.basic_processing import BasicProcessorManager, BasicProcessor
from cookiemonster.tests.processor._stubs import StubCookieJar
from cookiemonster.tests.processor._stubs import StubNotifier


class TestBasicProcessor(unittest.TestCase):
    """
    Tests for `BasicProcessor`.
    """
    FILE_IDENTIFIER = "abc"

    def setUp(self):
        self.cookie = Cookie(TestBasicProcessor.FILE_IDENTIFIER)
        self.rules = [Rule(lambda *args: False, lambda *args: RuleAction([], False)) for _ in range(10)]
        self.processor = BasicProcessor()

    def test_process_when_no_rules(self):
        self.rules = []

        def assertions(terminate_processing: bool, notifications: List[Notification]):
            self.assertFalse(terminate_processing)
            self.assertEquals(len(notifications), 0)

        self.processor.process(self.cookie, self.rules, TestBasicProcessor._create_assert_on_complete(assertions))

    def test_process_when_no_matched_rules(self):
        def assertions(terminate_processing: bool, notifications: List[Notification]):
            self.assertFalse(terminate_processing)
            self.assertEquals(len(notifications), 0)

        self.processor.process(self.cookie, self.rules, TestBasicProcessor._create_assert_on_complete(assertions))

    def test_process_when_matched_rules_and_no_termination(self):
        notifications = [Notification(i) for i in range(3)]

        self.rules.append(Rule(lambda *args: True, lambda *args: RuleAction([notifications[0], notifications[1]], False)))
        self.rules.append(Rule(lambda *args: True, lambda *args: RuleAction([notifications[2]], False)))

        def assertions(terminate_processing: bool, selected_notifications: List[Notification]):
            self.assertFalse(terminate_processing)
            self.assertCountEqual(selected_notifications, notifications)

        self.processor.process(self.cookie, self.rules, TestBasicProcessor._create_assert_on_complete(assertions))

    @unittest.skip("Running order of rules not yet established")
    def test_process_when_matched_rules_and_termination(self):
        pass


    @staticmethod
    def _create_assert_on_complete(assertions: Callable[[bool, List[Notification]], None]) \
            -> Callable[[bool, List[Notification]], None]:
        """
        TODO
        :param assertions:
        :return:
        """
        lock = Lock()
        lock.acquire()

        def on_complete(terminate_processing: bool, notifications: List[Notification]):
            assertions(terminate_processing, notifications)
            lock.release()

        Thread(target=lock.acquire).start()

        return on_complete



class TestBasicProcessorManager(unittest.TestCase):
    """
    Tests for `BasicProcessorManager`.
    """
    _NUMBER_OF_PROCESSORS = 5

    def setUp(self):
        self.cookie_jar = StubCookieJar()
        self.notifier = StubNotifier()
        self.rules = []
        self.enrichment_manager = EnrichmentManager()
        self.process_manager = BasicProcessorManager(TestBasicProcessorManager._NUMBER_OF_PROCESSORS, self.cookie_jar,
                                                     self.rules, self.enrichment_manager, self.notifier)

        self.cookie = Cookie("")
        self.rule = Rule(lambda information: True, lambda information: RuleAction(set(), True))
        self.rules.append(self.rule)

    def test_process_any_cookies_when_no_jobs(self):
        self.cookie_jar.get_next_for_processing = MagicMock(return_value=None)
        self.process_manager.on_cookie_processed = MagicMock()

        self.process_manager.process_any_cookies()
        self.cookie_jar.get_next_for_processing.assert_called_once_with()
        self.process_manager.on_cookie_processed.assert_not_called()

    def test_process_any_cookies_when_jobs_but_no_free_processors(self):
        zero_process_manager = BasicProcessorManager(
            0, self.cookie_jar, self.rules, self.enrichment_manager, self.notifier)
        self.cookie_jar.get_next_for_processing = MagicMock(return_value=self.cookie)
        zero_process_manager.on_cookie_processed = MagicMock()

        zero_process_manager.process_any_cookies()
        zero_process_manager.on_cookie_processed.assert_not_called()

    def test_process_any_cookies_when_jobs_and_free_processors(self):
        number_of_jobs = 50
        self.cookie_jar.get_next_for_processing = MagicMock(
            side_effect=[self.cookie for _ in range(number_of_jobs)] + [None for _ in range(100)])

        semaphore = Semaphore(0)

        def v_semaphore(*args):
            semaphore.release()

        self.process_manager.on_cookie_processed = MagicMock(side_effect=v_semaphore)

        self.process_manager.process_any_cookies()
        calls = [call(self.cookie, True, []) for _ in range(number_of_jobs)]

        for _ in range(number_of_jobs):
            semaphore.acquire()

        self.process_manager.on_cookie_processed.assert_has_calls(calls)

    def test_on_cookie_processed_when_no_rules_matched_and_no_more_data_can_be_loaded(self):
        self.cookie_jar.mark_as_reprocess = MagicMock()
        self.cookie_jar.mark_as_complete = MagicMock()
        self.notifier.do = MagicMock()

        self.process_manager.on_cookie_processed(self.cookie, False)
        self.cookie_jar.mark_as_reprocess.assert_not_called()
        self.cookie_jar.mark_as_complete.assert_called_once_with(self.cookie.path)
        self.notifier.do.assert_called_with(Notification("unknown", self.cookie.path))      # XXX

    def test_on_cookie_processed_when_no_rules_matched_and_but_data_can_be_loaded(self):
        enrichment = Enrichment("source", datetime.min, Metadata())
        data_loader = EnrichmentLoader(lambda *args: False, lambda *args: enrichment)
        self.enrichment_manager.data_loaders.append(data_loader)

        self.cookie_jar.mark_as_reprocess = MagicMock()
        self.cookie_jar.mark_as_complete = MagicMock()
        self.cookie_jar.enrich_cookie = MagicMock()
        self.notifier.do = MagicMock()

        self.process_manager.on_cookie_processed(self.cookie, False)
        self.cookie_jar.enrich_cookie.assert_called_once_with(self.cookie.path, enrichment)
        self.cookie_jar.mark_as_reprocess.assert_called_once_with(self.cookie.path)
        self.cookie_jar.mark_as_complete.assert_not_called()
        self.notifier.do.assert_not_called()

    def test_on_cookie_processed_when_rules_matched_and_terminate(self):
        notifications = [Notification("a", "b"), Notification("c", "d")]

        self.rules.remove(self.rule)
        assert len(self.rules) == 0

        self.cookie_jar.mark_as_reprocess = MagicMock()
        self.cookie_jar.mark_as_complete = MagicMock()
        self.notifier.do = MagicMock()

        self.process_manager.on_cookie_processed(self.cookie, True, notifications)
        self.cookie_jar.mark_as_reprocess.assert_not_called()
        self.cookie_jar.mark_as_complete.assert_called_once_with(self.cookie.path)
        self.notifier.do.assert_has_calls([call(notification) for notification in notifications], True)

    def test_on_cookie_processed_when_rules_matched_and_not_terminate_and_more_data(self):
        notifications = [Notification("a", "b"), Notification("c", "d")]
        enrichment = Enrichment("source", datetime.min, Metadata())
        enrichement_loader = EnrichmentLoader(lambda *args: False, lambda *args: enrichment)
        self.enrichment_manager.data_loaders.append(enrichement_loader)

        self.rules.remove(self.rule)
        assert len(self.rules) == 0

        self.cookie_jar.mark_as_reprocess = MagicMock()
        self.cookie_jar.mark_as_complete = MagicMock()
        self.cookie_jar.enrich_cookie = MagicMock()
        self.notifier.do = MagicMock()

        self.process_manager.on_cookie_processed(self.cookie, False, notifications)
        self.cookie_jar.enrich_cookie.assert_called_once_with(self.cookie.path, enrichment)
        self.cookie_jar.mark_as_reprocess.assert_called_once_with(self.cookie.path)
        self.cookie_jar.mark_as_complete.assert_not_called()
        self.notifier.do.assert_has_calls([call(notification) for notification in notifications], True)


if __name__ == "__main__":
    unittest.main()
