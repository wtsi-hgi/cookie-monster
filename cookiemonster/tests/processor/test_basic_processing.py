import unittest
from datetime import datetime
from unittest.mock import MagicMock, call

from multiprocessing import Semaphore

from cookiemonster.common.models import Cookie, Notification, Enrichment
from cookiemonster.processor._enrichment import EnrichmentManager
from cookiemonster.processor._models import Rule, EnrichmentLoader
from cookiemonster.processor._models import RuleAction
from cookiemonster.processor._rules import RulesManager
from cookiemonster.processor.basic_processing import BasicProcessorManager
from cookiemonster.tests.processor._stubs import StubCookieJar
from cookiemonster.tests.processor._stubs import StubNotifier


class TestBasicProcessorManager(unittest.TestCase):
    """
    Tests for `BasicProcessorManager`.
    """
    _NUMBER_OF_PROCESSORS = 5

    def setUp(self):
        self.cookie_jar = StubCookieJar()
        self.notifier = StubNotifier()
        self.rules_manager = RulesManager()
        self.data_loader_manager = EnrichmentManager()
        self.process_manager = BasicProcessorManager(
            TestBasicProcessorManager._NUMBER_OF_PROCESSORS, self.cookie_jar, self.rules_manager,
            self.data_loader_manager, self.notifier)

        self.cookie = Cookie("")
        self.rule = Rule(lambda information: True, lambda information: RuleAction(set(), True))
        self.rules_manager.add_rule(self.rule)

    def test_process_any_cookie_jobs_when_no_jobs(self):
        self.cookie_jar.get_next_for_processing = MagicMock(return_value=None)
        self.process_manager.on_cookie_processed = MagicMock()

        self.process_manager.process_any_cookie_jobs()
        self.cookie_jar.get_next_for_processing.assert_called_once_with()
        self.process_manager.on_cookie_processed.assert_not_called()

    def test_process_any_cookie_jobs_when_jobs_but_no_free_processors(self):
        zero_process_manager = BasicProcessorManager(
            0, self.cookie_jar, self.rules_manager, self.data_loader_manager, self.notifier)
        self.cookie_jar.get_next_for_processing = MagicMock(return_value=self.cookie)
        zero_process_manager.on_cookie_processed = MagicMock()

        zero_process_manager.process_any_cookie_jobs()
        zero_process_manager.on_cookie_processed.assert_not_called()

    def test_process_any_cookie_jobs_when_jobs_and_free_processors(self):
        number_of_jobs = 50
        self.cookie_jar.get_next_for_processing = MagicMock(
            side_effect=[self.cookie for _ in range(number_of_jobs)] + [None for _ in range(100)])

        semaphore = Semaphore(0)

        def v_semaphore(*args):
            semaphore.release()

        self.process_manager.on_cookie_processed = MagicMock(side_effect=v_semaphore)

        self.process_manager.process_any_cookie_jobs()
        calls = [call(self.cookie, True, set()) for _ in range(number_of_jobs)]

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
        self.notifier.do.assert_called_with(Notification("unknown", self.cookie.path))

    def test_on_cookie_processed_when_no_rules_matched_and_more_data_can_be_loaded(self):
        enrichment = Enrichment("source", datetime.min)
        data_loader = EnrichmentLoader(lambda *args: False, lambda *args: enrichment)
        self.data_loader_manager.data_loaders.append(data_loader)

        self.cookie_jar.mark_as_reprocess = MagicMock()
        self.cookie_jar.mark_as_complete = MagicMock()
        self.cookie_jar.enrich_metadata = MagicMock()
        self.notifier.do = MagicMock()

        self.process_manager.on_cookie_processed(self.cookie, False)
        self.cookie_jar.enrich_metadata.assert_called_once_with(self.cookie.path, enrichment)
        self.cookie_jar.mark_as_reprocess.assert_called_once_with(self.cookie.path)
        self.cookie_jar.mark_as_complete.assert_not_called()
        self.notifier.do.assert_not_called()

    def test_on_cookie_processed_when_rules_matched(self):
        notifications = [Notification("a", "b"), Notification("c", "d")]

        self.rules_manager.remove_rule(self.rule)
        assert len(self.rules_manager.get_rules()) == 0

        self.cookie_jar.mark_as_reprocess = MagicMock()
        self.cookie_jar.mark_as_complete = MagicMock()
        self.notifier.do = MagicMock()

        self.process_manager.on_cookie_processed(self.cookie, True, notifications)
        self.cookie_jar.mark_as_reprocess.assert_not_called()
        self.cookie_jar.mark_as_complete.assert_called_once_with(self.cookie.path)
        self.notifier.do.assert_has_calls([call(notification) for notification in notifications], True)
