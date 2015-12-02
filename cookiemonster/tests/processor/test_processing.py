import unittest

from cookiemonster.processor._models import Rule
from cookiemonster.processor.processing import RuleProcessingQueue
from cookiemonster.tests.processor._mocks import create_mock_rule


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
        self.assertEquals(len(list(rule_processing_queue.get_all())), len(self.rules))

    def test_has_unprocessed_rules_with_unprocessed_rules(self):
        rule_processing_queue = RuleProcessingQueue(self.rules)
        self.assertTrue(rule_processing_queue.has_unprocessed_rules())

    def test_has_unprocessed_rules_with_no_unprocessed_rules(self):
        rule_processing_queue = RuleProcessingQueue([])
        self.assertFalse(rule_processing_queue.has_unprocessed_rules())

    def test_get_next_when_next_exists(self):
        rule_processing_queue = RuleProcessingQueue(self.rules)
        self.assertIsInstance(rule_processing_queue.get_next_to_process(), Rule)

    def test_get_next_when_no_next_exists(self):
        rule_processing_queue = RuleProcessingQueue([])
        self.assertIsNone(rule_processing_queue.get_next_to_process())

    def test_mark_as_processed_unprocessed_rule(self):
        rule_processing_queue = RuleProcessingQueue(self.rules)

        processed_rules = []
        while rule_processing_queue.has_unprocessed_rules():
            rule = rule_processing_queue.get_next_to_process()
            self.assertNotIn(rule, processed_rules)
            rule_processing_queue.mark_as_processed(rule)
            processed_rules.append(rule)

    def test_mark_as_processed_processed_rule(self):
        rule_processing_queue = RuleProcessingQueue(self.rules)
        rule = rule_processing_queue.get_next_to_process()
        rule_processing_queue.mark_as_processed(rule)
        self.assertRaises(ValueError, rule_processing_queue.mark_as_processed, rule)

    def test_reset_all_marked_as_processed(self):
        rule_processing_queue = RuleProcessingQueue(self.rules)
        rule_processing_queue.mark_as_processed(rule_processing_queue.get_next_to_process())
        rule_processing_queue.mark_as_processed(rule_processing_queue.get_next_to_process())
        rule_processing_queue.reset_all_marked_as_processed()

        unprocessed_counter = 0
        while rule_processing_queue.has_unprocessed_rules():
            rule_processing_queue.mark_as_processed(rule_processing_queue.get_next_to_process())
            unprocessed_counter += 1
        self.assertEquals(unprocessed_counter, len(self.rules))


if __name__ == "__main__":
    unittest.main()
