import unittest
from datetime import datetime
from multiprocessing import Lock
from threading import Thread, Semaphore
from typing import Callable, List
from unittest.mock import MagicMock, call

from hgicommon.collections import Metadata
from hgicommon.data_source import ListDataSource
from hgicommon.mixable import Priority

from cookiemonster.common.models import Cookie, Notification, Enrichment
from cookiemonster.processor.basic_processing import BasicProcessorManager, BasicProcessor
from cookiemonster.processor.models import Rule, EnrichmentLoader, RuleAction
from cookiemonster.processor.processing import ABOUT_NO_RULES_MATCH
from cookiemonster.tests.processor._mocks import create_magic_mock_cookie_jar
from cookiemonster.tests.processor._stubs import StubNotificationReceiver

COOKIE_PATH = "/my/cookie"


class TestBasicProcessor(unittest.TestCase):
    """
    Tests for `BasicProcessor`.
    """
    def setUp(self):
        self.cookie = Cookie(COOKIE_PATH)
        self.rules = [Rule(lambda *args: False, lambda *args: RuleAction([], False)) for _ in range(10)]
        self.processor = BasicProcessor()

    def test_process_when_no_rules(self):
        def assertions(terminate_processing: bool, notifications: List[Notification]):
            self.assertFalse(terminate_processing)
            self.assertEqual(len(notifications), 0)

        self.processor.process(self.cookie, [], TestBasicProcessor._create_assert_on_complete(assertions))

    def test_process_when_no_matched_rules(self):
        def assertions(terminate_processing: bool, notifications: List[Notification]):
            self.assertFalse(terminate_processing)
            self.assertEqual(len(notifications), 0)

        self.processor.process(self.cookie, self.rules, TestBasicProcessor._create_assert_on_complete(assertions))

    def test_process_when_matched_rules_and_no_termination(self):
        notifications = [Notification(i) for i in range(3)]

        self.rules.append(Rule(
                lambda *args: True, lambda *args: RuleAction([notifications[0], notifications[1]], False)))
        self.rules.append(Rule(
                lambda *args: True, lambda *args: RuleAction([notifications[2]], False)))

        def assertions(terminate_processing: bool, selected_notifications: List[Notification]):
            self.assertFalse(terminate_processing)
            self.assertCountEqual(selected_notifications, notifications)

        self.processor.process(self.cookie, self.rules, TestBasicProcessor._create_assert_on_complete(assertions))

    def test_process_when_matched_rules_and_termination(self):
        notifications = [Notification(i) for i in range(2)]

        self.rules.append(Rule(
                lambda *args: True, lambda *args: RuleAction([notifications[0]], False), Priority.MIN_PRIORITY))
        self.rules.append(Rule(
                lambda *args: True, lambda *args: RuleAction([notifications[1]], True),
                Priority.get_lower_priority_value(Priority.MAX_PRIORITY)))
        self.rules.append(Rule(
                lambda *args: False, lambda *args: RuleAction([], True), Priority.MAX_PRIORITY))

        def assertions(terminate_processing: bool, selected_notifications: List[Notification]):
            self.assertTrue(terminate_processing)
            self.assertEqual(selected_notifications, [notifications[1]])

        self.processor.process(self.cookie, self.rules, TestBasicProcessor._create_assert_on_complete(assertions))

    def test_process_does_not_allow_rule_to_change_cookie_for_subsequent_rules(self):
        source = "my_enrichment"
        change_detected_in_next_rule = False

        def cookie_changer(cookie: Cookie) -> bool:
            enrichment = Enrichment(source, datetime(year=2000, month=1, day=1), Metadata())
            cookie.enrich(enrichment)
            return False

        def record_fail_if_changed(cookie: Cookie) -> bool:
            nonlocal change_detected_in_next_rule
            if source in cookie.get_metadata_sources():
                change_detected_in_next_rule = True

        rules = [
            Rule(cookie_changer, self.rules[0]._generate_action, priority=Priority.MAX_PRIORITY),
            Rule(record_fail_if_changed, self.rules[0]._generate_action, priority=Priority.MIN_PRIORITY)
        ]
        self.processor.process(self.cookie, rules, lambda *args: None)

        self.assertFalse(change_detected_in_next_rule)

    @staticmethod
    def _create_assert_on_complete(assertions: Callable[[bool, List[Notification]], None]) \
            -> Callable[[bool, List[Notification]], None]:
        """
        Creates assertions that are checked once processing has completed.
        :param assertions: function with the assertions that are to be made
        :return: the function to use as the `on_complete` with processors
        """
        lock = Lock()
        lock.acquire()
        # This child thread keeps everything alive until `on_complete` is called and executed
        Thread(target=lock.acquire).start()

        def on_complete(terminate_processing: bool, notifications: List[Notification]):
            assertions(terminate_processing, notifications)
            lock.release()

        return on_complete


class TestBasicProcessorManager(unittest.TestCase):
    """
    Tests for `BasicProcessorManager`.
    """
    _NUMBER_OF_PROCESSORS = 5

    def setUp(self):
        self.cookie_jar = create_magic_mock_cookie_jar()

        self.notification_receiver = StubNotificationReceiver()
        self.notification_receiver.receive = MagicMock()

        self.rules = []
        self.enrichment_loaders = []

        self.notifications = [Notification("a", "b"), Notification("c", "d")]
        self.cookie = Cookie(COOKIE_PATH)

        self.enrichment_loaders = self.enrichment_loaders
        self.processor_manager = BasicProcessorManager(
            TestBasicProcessorManager._NUMBER_OF_PROCESSORS, self.cookie_jar, ListDataSource(self.rules),
            ListDataSource(self.enrichment_loaders), ListDataSource([self.notification_receiver]))

    def test_init_with_less_than_one_processor(self):
        self.assertRaises(ValueError, BasicProcessorManager, 0, self.cookie_jar, ListDataSource(self.rules),
                          self.enrichment_loaders, self.notification_receiver)

    def test_process_any_cookies_when_no_jobs(self):
        self.processor_manager.process_any_cookies()

        self.cookie_jar.get_next_for_processing.assert_called_once_with()
        self.cookie_jar.mark_as_complete.assert_not_called()
        self.notification_receiver.receive.assert_not_called()

    def test_process_any_cookies_when_jobs(self):
        complete = Semaphore(0)

        def on_complete(*args):
            complete.release()

        self.cookie_jar.mark_as_complete = MagicMock(side_effect=on_complete)

        self.rules.append(Rule(lambda cookie: True, lambda cookie: RuleAction([], True)))

        number_to_process = 100
        for i in range(number_to_process):
            self.cookie_jar.mark_for_processing("%s/%s" % (COOKIE_PATH, i))
            Thread(target=self.processor_manager.process_any_cookies).start()

        completed = 0
        while completed != number_to_process:
            complete.acquire()
            completed += 1

    def test_process_any_cookies_when_no_free_processors(self):
        processor_manager = BasicProcessorManager(1, self.cookie_jar, ListDataSource(self.rules),
                                                  ListDataSource(self.enrichment_loaders),
                                                  ListDataSource([self.notification_receiver]))

        complete = Semaphore(0)

        def on_complete(*args):
            complete.release()

        self.cookie_jar.mark_as_complete = MagicMock(side_effect=on_complete)

        rule_lock = Semaphore(0)
        match_lock = Lock()
        match_lock.acquire()

        def matching_criteria(cookie: Cookie) -> bool:
            match_lock.release()
            rule_lock.acquire()
            return True

        self.rules.append(Rule(matching_criteria, lambda cookie: RuleAction([], True)))

        self.cookie_jar.mark_for_processing(self.cookie.path)
        processor_manager.process_any_cookies()
        match_lock.acquire()
        # Processor should have locked at this point - i.e. 0 free processors

        self.cookie_jar.mark_for_processing("/other/cookie")
        processor_manager.process_any_cookies()
        # The fact that there are more cookies should be "remembered" by the processor manager

        # Change the rules for the next cookie to be processed
        self.rules.pop()
        self.rules.append(Rule(lambda cookie: True, lambda cookie: RuleAction([], True)))

        # Free the processor to complete the first cookie
        rule_lock.release()
        rule_lock.release()

        # Wait for both cookies to be processed
        completed = 0
        while completed != 2:
            complete.acquire()
            completed += 1

        self.cookie_jar.mark_as_complete.assert_has_calls([call(self.cookie.path), call("/other/cookie")])
        self.notification_receiver.receive.assert_not_called()

    def test_on_cookie_processed_when_no_terminiation_no_enrichment(self):
        self.cookie_jar.mark_for_processing(self.cookie.path)
        self.cookie_jar.get_next_for_processing()

        self.processor_manager.on_cookie_processed(self.cookie, False, self.notifications)

        self.cookie_jar.mark_as_complete.assert_called_once_with(self.cookie.path)
        self.notification_receiver.receive.assert_has_calls(
                [call(notification) for notification in self.notifications] +
                [call(Notification(ABOUT_NO_RULES_MATCH, self.cookie.path, "BasicProcessorManager"))], True)

    def test_on_cookie_processed_when_no_termination_but_enrichment(self):
        enrichment = Enrichment("source", datetime.min, Metadata())
        enrichment_loader = EnrichmentLoader(lambda *args: True, lambda *args: enrichment)
        self.enrichment_loaders.append(enrichment_loader)

        self.processor_manager.on_cookie_processed(self.cookie, False, self.notifications)

        self.cookie_jar.enrich_cookie.assert_called_once_with(self.cookie.path, enrichment)
        self.cookie_jar.mark_for_processing.assert_called_once_with(self.cookie.path)
        self.cookie_jar.mark_as_complete.assert_not_called()
        self.notification_receiver.receive.assert_has_calls(
                [call(notification) for notification in self.notifications], True)

    def test_on_cookie_processed_when_termination(self):
        self.cookie_jar.mark_for_processing(self.cookie.path)
        self.cookie_jar.get_next_for_processing()

        self.processor_manager.on_cookie_processed(self.cookie, True, self.notifications)

        self.cookie_jar.mark_as_complete.assert_called_once_with(self.cookie.path)
        self.notification_receiver.receive.assert_has_calls(
                [call(notification) for notification in self.notifications], True)


if __name__ == "__main__":
    unittest.main()
