import unittest

from cookiemonster.rules._rules_management import RulesManager
from cookiemonster.rules._simple_manager import SimpleProcessorManager
from cookiemonster.tests.rules._stubs import StubCookieJar, StubNotifier


class TestSimpleProcessorManager(unittest.TestCase):
    """
    Tests for `SimpleProcessorManager`.
    """
    _NUMBER_OF_PROCESSORS = 5

    def setUp(self):
        self.cookie_jar = StubCookieJar()
        self.rules_manager = RulesManager()
        self.notifier = StubNotifier()
        self.process_manager = SimpleProcessorManager(
            TestSimpleProcessorManager._NUMBER_OF_PROCESSORS, self.cookie_jar, self.rules_manager, self.notifier)

    def test_process_any_jobs_when_no_jobs(self):
        self.process_manager.process_any_jobs()

        pass

    def test_process_any_jobs_when_jobs_and_free_processors(self):
        pass

    def test_process_any_jobs_when_jobs_but_no_free_processors(self):
        pass



if __name__ == '__main__':
    unittest.main()
