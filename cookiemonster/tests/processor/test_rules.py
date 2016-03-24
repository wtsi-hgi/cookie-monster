"""
Authors
-------
* Colin Nolan <cn13@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015, 2016 Genome Research Limited
"""
import unittest

from hgicommon.mixable import Priority
from typing import List

from cookiemonster import Rule
from cookiemonster.processor._rules import RuleQueue, RuleSource
from cookiemonster.tests.processor._mocks import create_mock_rule


class TestRuleQueue(unittest.TestCase):
    """
    Unit tests for `RuleQueue`.
    """
    def setUp(self):
        self.rules = []
        priority = Priority.MAX_PRIORITY
        for _ in range(10):
            self.rules.append(create_mock_rule(priority))
            priority = Priority.get_lower_priority_value(priority)

    def test_constructor(self):
        rule_queue = RuleQueue(self.rules)
        self.assertTrue(rule_queue.has_unapplied_rules())

        rules = []
        while rule_queue.has_unapplied_rules():
            rules.append(rule_queue.get_next())

        self.assertCountEqual(rules, self.rules)

    def test_has_unapplied_rules_with_unapplied_rules(self):
        rule_queue = RuleQueue(self.rules)
        self.assertTrue(rule_queue.has_unapplied_rules())

    def test_has_unapplied_rules_with_no_unapplied_rules(self):
        rule_queue = RuleQueue([])
        self.assertFalse(rule_queue.has_unapplied_rules())

    def test_get_next_when_no_next_exists(self):
        rule_queue = RuleQueue([])
        self.assertIsNone(rule_queue.get_next())

    def test_get_next_can_get_all_in_correct_order(self):
        rule_queue = RuleQueue(self.rules)

        rules = []  # type: List[Rule]
        while rule_queue.has_unapplied_rules():
            rule = rule_queue.get_next()

            for previous_rule in rules:
                self.assertGreaterEqual(
                    abs(Priority.MAX_PRIORITY - rule.priority), abs(Priority.MAX_PRIORITY - previous_rule.priority))

            self.assertIn(rule, self.rules)
            rules.append(rule)

        self.assertCountEqual(rules, self.rules)

    def test_mark_as_applied_unapplied_rule(self):
        rule_queue = RuleQueue(self.rules)

        applied_rules = []
        while rule_queue.has_unapplied_rules():
            rule = rule_queue.get_next()
            self.assertNotIn(rule, applied_rules)
            rule_queue.mark_as_applied(rule)
            applied_rules.append(rule)

    def test_mark_as_applied_applied(self):
        rule_queue = RuleQueue(self.rules)
        rule = rule_queue.get_next()
        rule_queue.mark_as_applied(rule)
        self.assertRaises(ValueError, rule_queue.mark_as_applied, rule)

    def test_mark_as_applied_applied_when_not_being_applied(self):
        rule_queue = RuleQueue(self.rules)
        rule = self.rules[0]
        self.assertRaises(ValueError, rule_queue.mark_as_applied, rule)

    def test_reset(self):
        rule_queue = RuleQueue(self.rules)
        rule_1 = rule_queue.get_next()
        rule_queue.mark_as_applied(rule_1)
        rule_2 = rule_queue.get_next()
        rule_queue.mark_as_applied(rule_2)
        rule_queue.reset()

        unapplied_counter = 0
        while rule_queue.has_unapplied_rules():
            rule = rule_queue.get_next()
            rule_queue.mark_as_applied(rule)
            unapplied_counter += 1
        self.assertEqual(unapplied_counter, len(self.rules))


class TestRuleSource(unittest.TestCase):
    """
    Tests for `RuleSource`.
    """
    def setUp(self):
        self.source = RuleSource("/")

    def test_is_data_file_when_is(self):
        self.assertTrue(self.source.is_data_file("/my/file.rule.py"))

    def test_is_data_file_when_is_not(self):
        self.assertFalse(self.source.is_data_file("/my/file.py"))


if __name__ == "__main__":
    unittest.main()
