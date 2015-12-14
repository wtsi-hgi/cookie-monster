import copy
import re
from multiprocessing import Lock
from queue import PriorityQueue
from typing import Iterable, Optional

from hgicommon.data_source import RegisteringDataSource

from cookiemonster.processor.models import Rule


class RuleProcessingQueue:
    """
    A queue of data that are to be processed.

    Thread-safe.
    """
    def __init__(self, rules: Iterable[Rule]):
        """
        Constructor.
        :param rules: the data to be processed (these will be copied so changes to the set outside of this object will
        have no effect)
        """
        self._rules = copy.copy(rules)
        self._not_processed = PriorityQueue()
        self._processed = []
        self._being_processed = []
        self._lists_lock = Lock()

        for rule in self._rules:
            self._not_processed.put(rule)

    def has_unprocessed_rules(self) -> bool:
        """
        Returns whether or not there exists rule that have not been processed.
        :return: whether there are data that have not been processed
        """
        return not self._not_processed.empty()

    def get_next_to_process(self) -> Optional[Rule]:
        """
        Gets the next rule that should be processed. Marks as currently being processed.
        :return: the next rule to be processed, else `None` if no more to process
        """
        if not self.has_unprocessed_rules():
            return None
        self._lists_lock.acquire()
        rule = self._not_processed.get()
        assert rule not in self._processed
        assert rule not in self._being_processed
        self._being_processed.append(rule)
        self._lists_lock.release()
        return rule

    def mark_as_processed(self, rule: Rule):
        """
        Marks the given rule as processed. Will raise a `ValueError` if the rule has already been marked as processed or
        if the rule is not marked as being processed (i.e. acquired via `get_next_to_process`).
        :param rule: the rule to mark as processed
        """
        if rule in self._processed:
            raise ValueError("Rule has already been marked as processed: %s" % rule)
        if rule not in self._being_processed:
            raise ValueError("Rule not marked as being processed: %s" % rule)
        self._lists_lock.acquire()
        self._being_processed.remove(rule)
        self._processed.append(rule)
        self._lists_lock.release()

    def reset_processed(self):
        """
        Resets all data previously marked as processed and those marked as being processed.
        """
        self._lists_lock.acquire()
        del self._being_processed[:]
        del self._processed[:]

        while not self._not_processed.empty():
            self._not_processed.get()
        for rule in self._rules:
            self._not_processed.put(rule)
        self._lists_lock.release()


class RulesSource(RegisteringDataSource):
    """
    Rule source where rules are registered from within Python modules within a given directory. These modules can be
    changed on-the-fly.
    """
    # Regex used to determine if a file contains a rule(s)
    FILE_PATH_MATCH_REGEX = ".*\.rule\.py"

    def __init__(self, directory_location: str):
        """
        Constructor.
        :param directory_location: the directory in which rules can be sourced from
        """
        super().__init__(directory_location, Rule)

    # Compiled `FILE_PATH_MATCH_REGEX`
    _compiled_file_path_match_regex = re.compile(FILE_PATH_MATCH_REGEX)

    def is_data_file(self, file_path: str) -> bool:
        return RulesSource._compiled_file_path_match_regex.search(file_path)
