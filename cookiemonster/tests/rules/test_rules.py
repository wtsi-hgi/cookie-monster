import unittest

from cookiemonster.rules._rules import RulesManager
from cookiemonster.tests.rules._mocks import create_mock_rule


class TestRulesManager(unittest.TestCase):
    """
    Unit tests for `RulesManager`.
    """
    def setUp(self):
        self.rules_manager = RulesManager()

    def test_add_rule(self):
        rule = create_mock_rule()
        self.rules_manager.add_rule(rule)
        self.assertEquals(self.rules_manager.get_rules(), [rule])

    def test_add_rule_that_has_been_already_added(self):
        rule = create_mock_rule()
        self.rules_manager.add_rule(rule)
        self.assertRaises(ValueError, self.rules_manager.add_rule, rule)

    def test_remove_rule(self):
        rule = create_mock_rule()
        self.rules_manager.add_rule(rule)
        assert rule in self.rules_manager.get_rules()
        self.rules_manager.remove_rule(rule)
        self.assertEquals(self.rules_manager.get_rules(), [])

    def test_remove_rule_that_does_not_exist(self):
        self.assertRaises(ValueError, self.rules_manager.remove_rule, create_mock_rule())


if __name__ == '__main__':
    unittest.main()
