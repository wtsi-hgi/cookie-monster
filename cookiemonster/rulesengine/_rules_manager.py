from typing import List

from cookiemonster.rulesengine._models import Rule


class RulesManager:
    """
    Manages the rules that are used by data processors.
    """
    def get_rules(self) -> List[Rule]:
        """
        Gets a list of rules that have been defined.
        :return: ordered set of rules
        """
        raise NotImplementedError()

    def add_rule(self, rule: Rule):
        """
        Adds a new rule.
        :param rule: the rule to add
        """
        raise NotImplementedError()

    def remove_rule(self, rule: Rule):
        """
        The rule to remove. Will raise a `ValueError` if the rule does not exist.
        :param rule: the rule to remove
        """
        raise NotImplementedError()


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
