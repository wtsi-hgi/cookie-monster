from typing import List

from cookiemonster.rulesengine._models import Rule


class RuleProcessingQueue:
    """
    A queue of rules that are to be processed.
    """
    def __init__(self, rules: List[Rule]):
        """
        Default constructor.
        :param rules: the rules to be processed
        """
        raise NotImplementedError()

    def exists_unprocessed_rules(self) -> bool:
        """
        Returns whether or not there exists rules that have not been processed.
        :return: whether there are rules that have not been processed
        """
        raise NotImplementedError()

    def get_next_unprocessed(self) -> Rule:
        """
        Gets the next rule that should be processed.
        :return: the next rule to be processed
        """
        raise NotImplementedError()

    def mark_as_processed(self, rule: Rule):
        """
        Marks the given rule as processed.
        :param rule: the rule to mark as processed
        """
        raise NotImplementedError()

    def reset_all_marked_as_processed(self):
        """
        Resets all rules previously marked as processed.
        """
        raise NotImplementedError()


class DataEnvironment(dict):
    """
    The environment that holds the data that has been loaded and is available for use when evaluating a rule.
    """
    pass

