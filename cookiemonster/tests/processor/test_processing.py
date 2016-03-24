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
from unittest.mock import MagicMock

from cookiemonster import Cookie, RuleAction
from cookiemonster.tests.processor._stubs import StubProcessor


class TestProcessor(unittest.TestCase):
    """
    Tests for `Processor`.
    """
    def setUp(self):
        self.cookie = Cookie("id")
        self.rule_actions = [RuleAction([])]

        self.processor = StubProcessor()
        self.processor.evaluate_rules_with_cookie = MagicMock(return_value=self.rule_actions)
        self.processor.execute_rule_actions = MagicMock()
        self.processor.handle_cookie_enrichment = MagicMock()

    def test_process_cookie_when_no_termination(self):
        self.processor.process_cookie(self.cookie)

        self.processor.evaluate_rules_with_cookie.assert_called_once_with(self.cookie)
        self.processor.execute_rule_actions.assert_called_once_with(self.rule_actions)
        self.processor.handle_cookie_enrichment.assert_called_once_with(self.cookie)

    def test_process_cookie_when_termination(self):
        self.rule_actions.append(RuleAction([], True))
        self.processor.process_cookie(self.cookie)

        self.processor.evaluate_rules_with_cookie.assert_called_once_with(self.cookie)
        self.processor.execute_rule_actions.assert_called_once_with(self.rule_actions)
        self.processor.handle_cookie_enrichment.assert_not_called()


if __name__ == "__main__":
    unittest.main()
