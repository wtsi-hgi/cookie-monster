"""
Legalese
--------
Copyright (c) 2015, 2016 Genome Research Ltd.

Author: Colin Nolan <cn13@sanger.ac.uk>

This file is part of Cookie Monster.

Cookie Monster is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by the
Free Software Foundation; either version 3 of the License, or (at your
option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License along
with this program. If not, see <http://www.gnu.org/licenses/>.
"""
import unittest
from datetime import datetime
from threading import Semaphore, Thread, Lock
from unittest.mock import MagicMock, call

from hgicommon.collections import Metadata
from hgicommon.data_source import ListDataSource
from hgicommon.mixable import Priority

from cookiemonster.common.models import Cookie, Notification, Enrichment
from cookiemonster.common.context import Context
from cookiemonster.cookiejar.in_memory_cookiejar import InMemoryCookieJar
from cookiemonster.processor.basic_processing import BasicProcessor, BasicProcessorManager
from cookiemonster.processor.models import Rule, EnrichmentLoader
from cookiemonster.tests.processor._mocks import create_magic_mock_cookie_jar

COOKIE_IDENTIFIER = "/my/cookie"
RULE_IDENTIFIER = "my_rule"
SAMPLE_ENRICHMENT = Enrichment("sample", datetime(year=2000, month=1, day=1), Metadata())


class TestBasicProcessor(unittest.TestCase):
    """
    Tests for `BasicProcessor`.
    """
    def setUp(self):
        self.cookie_jar = InMemoryCookieJar()
        self.rules = [Rule(lambda *args: False, MagicMock(), RULE_IDENTIFIER) for _ in range(10)]
        self.cookie = Cookie(COOKIE_IDENTIFIER)
        self.processor = BasicProcessor(self.cookie_jar, [], [])

    def test_evaluate_rules_with_cookie_when_no_rules(self):
        halt = self.processor.evaluate_rules_with_cookie(self.cookie)

        self.assertFalse(halt)

    def test_evaluate_rules_with_cookie_when_no_matched_rules(self):
        self.processor.rules = self.rules
        halt = self.processor.evaluate_rules_with_cookie(self.cookie)

        for rule in self.rules:
            rule._action.assert_not_called()
        self.assertFalse(halt)

    def test_evaluate_rules_with_cookie_when_matched_rules_and_no_termination(self):
        extra_rules = [
            Rule(lambda *args: True, MagicMock(return_value=False), RULE_IDENTIFIER),
            Rule(lambda *args: True, MagicMock(return_value=False), RULE_IDENTIFIER)
        ]
        self.processor.rules = self.rules + extra_rules
        halt = self.processor.evaluate_rules_with_cookie(self.cookie)

        for rule in self.rules:
            rule._action.assert_not_called()
        self.assertFalse(halt)

    def test_evaluate_rules_with_cookie_when_matched_rules_and_termination(self):
        production_to_be_called = MagicMock(return_value=True)
        extra_rules = [
            Rule(lambda *args: True, lambda *args: False, Priority.MIN_PRIORITY),
            Rule(lambda *args: True, production_to_be_called,
                 Priority.get_lower_priority_value(Priority.MAX_PRIORITY)),
            Rule(lambda *args: False, lambda *args: False, Priority.MAX_PRIORITY)
        ]
        self.processor.rules = self.rules + extra_rules
        halt = self.processor.evaluate_rules_with_cookie(self.cookie)

        total_call_count = 0
        for rule in self.rules:
            total_call_count += rule._action.call_count
        self.assertEqual(total_call_count, 0)
        self.assertEqual(production_to_be_called.call_count, 1)
        self.assertTrue(halt)

    def test_evaluate_rules_with_cookie_does_not_allow_rule_to_change_cookie_for_subsequent_rules(self):
        source = "my_enrichment"
        change_detected_in_next_rule = False

        def cookie_changer(cookie: Cookie) -> bool:
            enrichment = Enrichment(source, datetime(year=2000, month=1, day=1), Metadata())
            cookie.enrich(enrichment)
            return False

        def record_fail_if_changed(cookie: Cookie) -> bool:
            nonlocal change_detected_in_next_rule
            if source in cookie.get_enrichment_sources():
                change_detected_in_next_rule = True

        self.processor.rules = [
            Rule(cookie_changer, self.rules[0]._action, RULE_IDENTIFIER, priority=Priority.MAX_PRIORITY),
            Rule(record_fail_if_changed, self.rules[0]._action, RULE_IDENTIFIER, priority=Priority.MIN_PRIORITY)
        ]
        self.processor.process_cookie(self.cookie)

        self.assertFalse(change_detected_in_next_rule)

    def test_handle_cookie_enrichment_when_matching_enrichments(self):
        self.processor.notification_receivers = [MagicMock()]
        self.processor.enrichment_loaders = [EnrichmentLoader(
            lambda *args: True, lambda *args: SAMPLE_ENRICHMENT, RULE_IDENTIFIER)]

        self.processor.handle_cookie_enrichment(self.cookie)

        self.processor.notification_receivers[0].receive.assert_not_called()
        cookie = self.cookie_jar.get_next_for_processing()
        self.assertIn(SAMPLE_ENRICHMENT, cookie.enrichments)


class TestBasicProcessorManager(unittest.TestCase):
    """
    Tests for `BasicProcessorManager`.
    """
    def setUp(self):
        self.cookie_jar = create_magic_mock_cookie_jar()

        self.rules = []
        self.enrichment_loaders = []

        self.notifications = [Notification("a", "b"), Notification("c", "d")]
        self.cookie = Cookie(COOKIE_IDENTIFIER)

        self.enrichment_loaders = self.enrichment_loaders
        self.processor_manager = BasicProcessorManager(
            self.cookie_jar, ListDataSource(self.rules), ListDataSource(self.enrichment_loaders))

    def test_init_with_less_than_one_thread(self):
        self.assertRaises(
            ValueError, BasicProcessorManager, self.cookie_jar, ListDataSource(self.rules), self.enrichment_loaders, 0)

    def test_process_any_cookies_when_no_jobs(self):
        complete = Lock()
        complete.acquire()

        def on_get_next_for_processing(*args):
            complete.release()

        self.cookie_jar.get_next_for_processing = MagicMock(side_effect=on_get_next_for_processing)

        self.processor_manager.process_any_cookies()
        complete.acquire()

        self.cookie_jar.get_next_for_processing.assert_called_once_with()
        self.cookie_jar.mark_as_complete.assert_not_called()

    def test_process_any_cookies_when_jobs(self):
        complete = Semaphore(0)

        def on_complete(*args):
            complete.release()

        self.cookie_jar.mark_as_complete = MagicMock(side_effect=on_complete)

        self.rules.append(Rule(lambda *args: True, lambda *args: True, RULE_IDENTIFIER))

        number_to_process = 100
        for i in range(number_to_process):
            self.cookie_jar.mark_for_processing("%s/%s" % (COOKIE_IDENTIFIER, i))
            Thread(target=self.processor_manager.process_any_cookies).start()

        completed = 0
        while completed != number_to_process:
            complete.acquire()
            completed += 1

    def test_process_any_cookies_when_no_processing_resources(self):
        processor_manager = BasicProcessorManager(
            self.cookie_jar, ListDataSource(self.rules), ListDataSource(self.enrichment_loaders))

        complete = Semaphore(0)

        def on_complete(*args):
            complete.release()

        self.cookie_jar.mark_as_complete = MagicMock(side_effect=on_complete)

        rule_lock = Semaphore(0)
        match_lock = Lock()
        match_lock.acquire()

        def matching_criteria(cookie: Cookie, _:Context) -> bool:
            match_lock.release()
            rule_lock.acquire()
            return True

        self.rules.append(Rule(matching_criteria, lambda *args: True, RULE_IDENTIFIER))

        self.cookie_jar.mark_for_processing(self.cookie.identifier)
        processor_manager.process_any_cookies()
        match_lock.acquire()
        # Processor should have locked at this point - i.e. 0 free processors

        self.cookie_jar.mark_for_processing("/other/cookie")
        processor_manager.process_any_cookies()
        # The fact that there are more cookies should be "remembered" by the processor manager

        # Change the rules for the next cookie to be processed
        self.rules.pop()
        rule_execute_monitor = MagicMock()
        self.rules.append(Rule(lambda *args: True, rule_execute_monitor, RULE_IDENTIFIER))

        # Free the processor to complete the first cookie
        rule_lock.release()
        rule_lock.release()

        # Wait for both cookies to be processed
        completed = 0
        while completed != 2:
            complete.acquire()
            completed += 1

        self.cookie_jar.mark_as_complete.assert_has_calls([call(self.cookie.identifier), call("/other/cookie")])
        assert len(self.rules) == 1
        self.assertEqual(rule_execute_monitor.call_count, 1)


if __name__ == "__main__":
    unittest.main()
