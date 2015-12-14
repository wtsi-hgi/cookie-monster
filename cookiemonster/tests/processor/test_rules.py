import unittest

from hgicommon.mixable import Priority
from typing import List

from cookiemonster import Rule
from cookiemonster.processor._rules import RuleProcessingQueue, RulesSource
from cookiemonster.tests.processor._mocks import create_mock_rule


class TestRuleProcessingQueue(unittest.TestCase):
    """
    Unit tests for `RuleProcessingQueue`.
    """
    def setUp(self):
        self.rules = []
        priority = Priority.MAX_PRIORITY
        for _ in range(10):
            self.rules.append(create_mock_rule(priority))
            priority = Priority.get_lower_priority_value(priority)

    def test_constructor(self):
        rule_processing_queue = RuleProcessingQueue(self.rules)
        self.assertTrue(rule_processing_queue.has_unprocessed_rules())

        rules = []
        while rule_processing_queue.has_unprocessed_rules():
            rules.append(rule_processing_queue.get_next_to_process())

        self.assertCountEqual(rules, self.rules)

    def test_has_unprocessed_rules_with_unprocessed_rules(self):
        rule_processing_queue = RuleProcessingQueue(self.rules)
        self.assertTrue(rule_processing_queue.has_unprocessed_rules())

    def test_has_unprocessed_rules_with_no_unprocessed_rules(self):
        rule_processing_queue = RuleProcessingQueue([])
        self.assertFalse(rule_processing_queue.has_unprocessed_rules())

    def test_get_next_when_no_next_exists(self):
        rule_processing_queue = RuleProcessingQueue([])
        self.assertIsNone(rule_processing_queue.get_next_to_process())

    def test_get_next_can_get_all_in_correct_order(self):
        rule_processing_queue = RuleProcessingQueue(self.rules)

        rules = []  # type: List[Rule]
        while rule_processing_queue.has_unprocessed_rules():
            rule = rule_processing_queue.get_next_to_process()

            for previous_rule in rules:
                self.assertGreaterEqual(
                    abs(Priority.MAX_PRIORITY - rule.priority), abs(Priority.MAX_PRIORITY - previous_rule.priority))

            self.assertIn(rule, self.rules)
            rules.append(rule)

        self.assertCountEqual(rules, self.rules)

    def test_mark_as_processed_unprocessed_rule(self):
        rule_processing_queue = RuleProcessingQueue(self.rules)

        processed_rules = []
        while rule_processing_queue.has_unprocessed_rules():
            rule = rule_processing_queue.get_next_to_process()
            self.assertNotIn(rule, processed_rules)
            rule_processing_queue.mark_as_processed(rule)
            processed_rules.append(rule)

    def test_mark_as_processed_processed(self):
        rule_processing_queue = RuleProcessingQueue(self.rules)
        rule = rule_processing_queue.get_next_to_process()
        rule_processing_queue.mark_as_processed(rule)
        self.assertRaises(ValueError, rule_processing_queue.mark_as_processed, rule)

    def test_mark_as_processed_processed_when_not_being_processed(self):
        rule_processing_queue = RuleProcessingQueue(self.rules)
        rule = self.rules[0]
        self.assertRaises(ValueError, rule_processing_queue.mark_as_processed, rule)

    def test_reset(self):
        rule_processing_queue = RuleProcessingQueue(self.rules)
        rule_1 = rule_processing_queue.get_next_to_process()
        rule_processing_queue.mark_as_processed(rule_1)
        rule_2 = rule_processing_queue.get_next_to_process()
        rule_processing_queue.mark_as_processed(rule_2)
        rule_processing_queue.reset_processed()

        unprocessed_counter = 0
        while rule_processing_queue.has_unprocessed_rules():
            rule = rule_processing_queue.get_next_to_process()
            rule_processing_queue.mark_as_processed(rule)
            unprocessed_counter += 1
        self.assertEquals(unprocessed_counter, len(self.rules))


class TestRulesSource(unittest.TestCase):
    """
    Tests for `RulesSource`.
    """
    def setUp(self):
        self.source = RulesSource("/")

    def test_is_data_file_when_is(self):
        self.assertTrue(self.source.is_data_file("/my/file.rule.py"))

    def test_is_data_file_when_is_not(self):
        self.assertFalse(self.source.is_data_file("/my/file.py"))


if __name__ == "__main__":
    unittest.main()
