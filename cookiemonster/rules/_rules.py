import copy

from cookiemonster.rules._collections import RuleCollection
from cookiemonster.rules._models import Rule


class RulesManager:
    """
    Manages the rules that are used by data processors.
    """
    def __init__(self):
        self._rules = RuleCollection()

    def get_rules(self) -> RuleCollection:
        """
        Gets a copy of the collection of rules that have been defined.
        :return: clopy of the collection of rules
        """
        return copy.deepcopy(self._rules)

    def add_rule(self, rule: Rule):
        """
        Adds a new rule.
        :param rule: the rule to add
        """
        if rule in self._rules:
            raise ValueError("Rule has already been defined: %s" % rule)
        self._rules.append(rule)

    def remove_rule(self, rule: Rule):
        """
        The rule to remove. Will raise a `ValueError` if the rule does not exist.
        :param rule: the rule to remove
        """
        if rule not in self._rules:
            raise ValueError("Rule has not been defined: %s" % rule)
        self._rules.remove(rule)


class InFileRulesMonitor:
    """
    Whether changes to the rules, defined by changes in the files in the directory.
    """
    def __init__(self, directory_location: str, rules_manager: RulesManager):
        """
        Default constructor.
        :param directory_location: the location of the rules
        :param rules_manager: the rules manager to update about changes in the rules
        """
        raise NotImplementedError()

    def is_monitoring(self) -> bool:
        """
        Whether this monitor is monitoring.
        :return: state of the monitor
        """
        raise NotImplementedError()

    def start(self):
        """
        Starts monitoring rules in the directory.
        """
        raise NotImplementedError()

    def stop(self):
        """
        Stops monitoring rules in the directory.
        """
        raise NotImplementedError()