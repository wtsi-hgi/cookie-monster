import unittest
from threading import Semaphore

from mock import MagicMock, call

from cookiemonster.common.models import CookieProcessState, Cookie, Notification, CookieCrumbs
from cookiemonster.processor._data_management import DataManager
from cookiemonster.processor._models import Rule, RuleAction, DataLoader
from cookiemonster.processor._rules_management import RulesManager
from cookiemonster.processor._simple_manager import SimpleProcessorManager
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
            side_effect=[self.job for _ in range(number_of_jobs)]
                        + [None for _ in range(TestSimpleProcessorManager._NUMBER_OF_PROCESSORS)])

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

        self.cookie_jar.mark_as_reprocess = MagicMock()
        self.cookie_jar.mark_as_complete = MagicMock()
        self.notifier.do = MagicMock()

        self.process_manager.on_job_processed(self.job, True, notifications)
        self.cookie_jar.mark_as_reprocess.assert_not_called()
        self.cookie_jar.mark_as_complete.assert_called_once_with(self.job.path)
        self.notifier.do.assert_has_calls([call(notification) for notification in notifications])


if __name__ == "__main__":
    unittest.main()
