import unittest
from threading import Semaphore

from mock import MagicMock, call

from cookiemonster.common.models import CookieProcessState, Cookie, Notification, CookieCrumbs
from cookiemonster.processor._data_management import DataManager
from cookiemonster.processor._models import RuleAction, DataLoader, Rule
from cookiemonster.processor._rules_management import RulesManager
from cookiemonster.processor.simple_processor import SimpleProcessorManager
from cookiemonster.processor.processor import RuleProcessingQueue
from cookiemonster.tests.processor._mocks import create_mock_rule
from cookiemonster.tests.processor._stubs import StubCookieJar, StubNotifier


class TestSimpleProcessorManager(unittest.TestCase):
    """
    Tests for `SimpleProcessorManager`.
    """
    _NUMBER_OF_PROCESSORS = 5

    def setUp(self):
        self.cookie_jar = StubCookieJar()
        self.notifier = StubNotifier()
        self.rules_manager = RulesManager()
        self.data_manager = DataManager()
        self.process_manager = SimpleProcessorManager(
            TestSimpleProcessorManager._NUMBER_OF_PROCESSORS, self.cookie_jar, self.rules_manager, self.data_manager,
            self.notifier)

        self.job = CookieProcessState(Cookie(""))
        self.rule = Rule(lambda information: True, lambda information: RuleAction(set(), True))
        self.rules_manager.add_rule(self.rule)

    def test_process_any_jobs_when_no_jobs(self):
        self.cookie_jar.get_next_for_processing = MagicMock(return_value=None)
        self.process_manager.on_job_processed = MagicMock()

        self.process_manager.process_any_jobs()
        self.cookie_jar.get_next_for_processing.assert_called_once_with()
        self.process_manager.on_job_processed.assert_not_called()

    def test_process_any_jobs_when_jobs_but_no_free_processors(self):
        zero_process_manager = SimpleProcessorManager(
            0, self.cookie_jar, self.rules_manager, self.data_manager, self.notifier)
        self.cookie_jar.get_next_for_processing = MagicMock(return_value=self.job)
        zero_process_manager.on_job_processed = MagicMock()

        zero_process_manager.process_any_jobs()
        zero_process_manager.on_job_processed.assert_not_called()

    def test_process_any_jobs_when_jobs_and_free_processors(self):
        number_of_jobs = 50
        self.cookie_jar.get_next_for_processing = MagicMock(
            side_effect=[self.job for _ in range(number_of_jobs)] + [None for _ in range(100)])

        semaphore = Semaphore(0)

        def v_semaphore(*args):
            semaphore.release()

        self.process_manager.on_job_processed = MagicMock(side_effect=v_semaphore)

        self.process_manager.process_any_jobs()
        calls = [call(self.job, True, set()) for _ in range(number_of_jobs)]

        for _ in range(number_of_jobs):
            semaphore.acquire()

        self.process_manager.on_job_processed.assert_has_calls(calls)

    def test_on_job_processed_when_no_rules_matched_and_no_more_data_can_be_loaded(self):
        self.cookie_jar.mark_as_reprocess = MagicMock()
        self.cookie_jar.mark_as_complete = MagicMock()
        self.notifier.do = MagicMock()

        self.process_manager.on_job_processed(self.job, False)
        self.cookie_jar.mark_as_reprocess.assert_not_called()
        self.cookie_jar.mark_as_complete.assert_called_once_with(self.job.path)
        self.notifier.do.assert_called_with(Notification("unknown", self.job.path))

    def test_on_job_processed_when_no_rules_matched_and_more_data_can_be_loaded(self):
        more_data = CookieCrumbs()
        data_loader = DataLoader(lambda *args: False, lambda *args: more_data)
        self.data_manager.data_loaders.append(data_loader)

        self.cookie_jar.mark_as_reprocess = MagicMock()
        self.cookie_jar.mark_as_complete = MagicMock()
        self.cookie_jar.enrich_metadata = MagicMock()
        self.notifier.do = MagicMock()

        self.process_manager.on_job_processed(self.job, False)
        self.cookie_jar.enrich_metadata.assert_called_once_with(self.job.path, more_data)
        self.cookie_jar.mark_as_reprocess.assert_called_once_with(self.job.path)
        self.cookie_jar.mark_as_complete.assert_not_called()
        self.notifier.do.assert_not_called()

    def test_on_job_processed_when_rules_matched(self):
        notifications = [Notification("a", "b"), Notification("c", "d")]

        self.rules_manager.remove_rule(self.rule)
        assert len(self.rules_manager.get_rules()) == 0

        self.cookie_jar.mark_as_reprocess = MagicMock()
        self.cookie_jar.mark_as_complete = MagicMock()
        self.notifier.do = MagicMock()

        self.process_manager.on_job_processed(self.job, True, notifications)
        self.cookie_jar.mark_as_reprocess.assert_not_called()
        self.cookie_jar.mark_as_complete.assert_called_once_with(self.job.path)
        self.notifier.do.assert_has_calls([call(notification) for notification in notifications], True)


class TestRuleProcessingQueue(unittest.TestCase):
    """
    Unit tests for `RuleProcessingQueue`.
    """
    def setUp(self):
        self.rules = set()
        for i in range(10):
            self.rules.add(create_mock_rule(i))

    def test_constructor(self):
        rule_processing_queue = RuleProcessingQueue(self.rules)
        self.assertTrue(rule_processing_queue.has_unprocessed_rules())
        self.assertEquals(len(rule_processing_queue.get_all()), len(self.rules))

    def test_has_unprocessed_rules_with_unprocessed_rules(self):
        rule_processing_queue = RuleProcessingQueue(self.rules)
        self.assertTrue(rule_processing_queue.has_unprocessed_rules())

    def test_has_unprocessed_rules_with_no_unprocessed_rules(self):
        rule_processing_queue = RuleProcessingQueue(set())
        self.assertFalse(rule_processing_queue.has_unprocessed_rules())

    def test_get_next_when_next_exists(self):
        rule_processing_queue = RuleProcessingQueue(self.rules)
        self.assertIsInstance(rule_processing_queue.get_next_unprocessed(), Rule)

    def test_get_next_when_no_next_exists(self):
        rule_processing_queue = RuleProcessingQueue(set())
        self.assertIsNone(rule_processing_queue.get_next_unprocessed())

    def test_mark_as_processed_unprocessed_rule(self):
        rule_processing_queue = RuleProcessingQueue(self.rules)

        processed_rules = []
        while rule_processing_queue.has_unprocessed_rules():
            rule = rule_processing_queue.get_next_unprocessed()
            self.assertNotIn(rule, processed_rules)
            rule_processing_queue.mark_as_processed(rule)
            processed_rules.append(rule)

    def test_mark_as_processed_processed_rule(self):
        rule_processing_queue = RuleProcessingQueue(self.rules)
        rule_processing_queue.mark_as_processed(list(self.rules)[0])
        self.assertRaises(ValueError, rule_processing_queue.mark_as_processed, list(self.rules)[0])

    def test_reset_all_marked_as_processed(self):
        rule_processing_queue = RuleProcessingQueue(self.rules)
        rule_processing_queue.mark_as_processed(list(self.rules)[0])
        rule_processing_queue.mark_as_processed(list(self.rules)[1])
        rule_processing_queue.reset_all_marked_as_processed()

        unprocessed_counter = 0
        while rule_processing_queue.has_unprocessed_rules():
            rule_processing_queue.mark_as_processed(rule_processing_queue.get_next_unprocessed())
            unprocessed_counter += 1
        self.assertEquals(unprocessed_counter, len(self.rules))


if __name__ == "__main__":
    unittest.main()
