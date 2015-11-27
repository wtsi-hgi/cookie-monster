import copy
from typing import Set

from cookiemonster.processor._models import Rule


class RulesManager:
    """
    Manages the processor that are used by data processors.
    """
    def __init__(self):
        self._rules = set()

    def get_rules(self) -> Set[Rule]:
        """
        Gets a copy of the collection of processor that have been defined.
        :return: clopy of the collection of processor
        """
        return copy.deepcopy(self._rules)

    def add_rule(self, rule: Rule):
        """
        Adds a new rule.
        :param rule: the rule to add
        """
        if rule in self._rules:
            raise ValueError("Rule has already been defined: %s" % rule)
        self._rules.add(rule)

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
    Whether changes to the processor, defined by changes in the files in the directory.
    """
    def __init__(self, directory_location: str, rules_manager: RulesManager):
        """
        Default constructor.
        :param directory_location: the location of the processor
        :param rules_manager: the processor manager to update about changes in the processor
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
        Starts monitoring processor in the directory.
        """
        raise NotImplementedError()

    def stop(self):
        """
        Stops monitoring processor in the directory.
        """
        raise NotImplementedError()
