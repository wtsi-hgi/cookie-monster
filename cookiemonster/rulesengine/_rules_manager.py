from typing import List

from cookiemonster.rulesengine.models import Rule


class RulesManager:
    """
    Manages the rules that are used by data processors.
    """
    def get_rules(self) -> List[Rule]:
        """
        Gets a list of rules that have been defined.
        :return: ordered set of rules
        """
        pass

    def add_rule(self, rule: Rule):
        """
        Adds a new rule.
        :param rule: the rule to add
        """
        pass

    def remove_rule(self, rule: Rule):
        """
        The rule to remove. Will raise a `ValueError` if the rule does not exist.
        :param rule: the rule to remove
        """
        pass


