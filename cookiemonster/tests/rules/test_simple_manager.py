import unittest
from threading import Semaphore

from mock import MagicMock, call

from cookiemonster.common.models import CookieProcessState, Cookie
from cookiemonster.rules._models import Rule, RuleAction
from cookiemonster.rules._rules_management import RulesManager
from cookiemonster.rules._simple_manager import SimpleProcessorManager
from cookiemonster.tests.rules._stubs import StubCookieJar, StubNotifier


class TestSimpleProcessorManager(unittest.TestCase):
    """
    Tests for `SimpleProcessorManager`.
    """
    _NUMBER_OF_PROCESSORS = 5

    def setUp(self):
        self.data_manager = StubCookieJar()
        self.notifier = StubNotifier()
        self.rules_manager = RulesManager()
        self.process_manager = SimpleProcessorManager(
            TestSimpleProcessorManager._NUMBER_OF_PROCESSORS, self.data_manager, self.rules_manager, self.notifier)

        self.job = CookieProcessState(Cookie(""))
        self.rule = Rule(lambda information: True, lambda information: RuleAction(set(), True))
        self.rules_manager.add_rule(self.rule)

    def test_process_any_jobs_when_no_jobs(self):
        self.data_manager.get_next_for_processing = MagicMock(return_value=None)
        self.process_manager.on_job_processed = MagicMock()

        self.process_manager.process_any_jobs()
        self.data_manager.get_next_for_processing.assert_called_once_with()
        self.process_manager.on_job_processed.assert_not_called()

    def test_process_any_jobs_when_jobs_but_no_free_processors(self):
        zero_process_manager = SimpleProcessorManager(0, self.data_manager, self.rules_manager, self.notifier)
        self.data_manager.get_next_for_processing = MagicMock(return_value=self.job)
        zero_process_manager.on_job_processed = MagicMock()

        zero_process_manager.process_any_jobs()
        zero_process_manager.on_job_processed.assert_not_called()

    def test_process_any_jobs_when_jobs_and_free_processors(self):
        number_of_jobs = 50
        self.data_manager.get_next_for_processing = MagicMock(
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

    def test_on_job_processed_when_no_rules_matched(self):
        # self.data_manager.
        # self.process_manager.on_job_processed(self.job, False)
        pass


if __name__ == '__main__':
    unittest.main()
