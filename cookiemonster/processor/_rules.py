"""
Authors
-------
* Colin Nolan <cn13@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015, 2016 Genome Research Limited
"""
import copy
import re
from multiprocessing import Lock
from queue import PriorityQueue
from typing import Iterable, Optional

from hgicommon.data_source import RegisteringDataSource

from cookiemonster.common.resource_accessor import ResourceAccessorContainerRegisteringDataSource, ResourceAccessor
from cookiemonster.processor.models import Rule


class RuleQueue:
    """
    A priority queue of rules that are used when processing a cookie.

    Thread-safe.
    """
    def __init__(self, rules: Iterable[Rule]):
        """
        Constructor.
        :param rules: the data to be processed (immutable copy made)
        """
        self._rules = copy.copy(rules)
        self._not_applied = PriorityQueue()
        self._applied = []
        self._being_applied = []
        self._lists_lock = Lock()

        for rule in self._rules:
            self._not_applied.put(rule)

    def has_unapplied_rules(self) -> bool:
        """
        Returns whether or not there exists rule that have not been applied.
        :return: whether there are data that have not been applied
        """
        return not self._not_applied.empty()

    def get_next(self) -> Optional[Rule]:
        """
        Gets the next rule that should be applied. Marks as currently being applied.

        Thread-safe.
        :return: the next rule to be processed, else `None` if no more to apply
        """
        if not self.has_unapplied_rules():
            return None
        with self._lists_lock:
            rule = self._not_applied.get()
            assert rule not in self._applied
            assert rule not in self._being_applied
            self._being_applied.append(rule)
        return rule

    def mark_as_applied(self, rule: Rule):
        """
        Marks the given rule as applied.

        Will raise a `ValueError` if the rule has already been marked as applied or if the rule is not marked as being
        applied (i.e. not acquired via `get_next`).

        Thread-safe.
        :param rule: the rule to mark as applied
        """
        if rule in self._applied:
            raise ValueError("Rule has already been marked as applied: %s" % rule)
        if rule not in self._being_applied:
            raise ValueError("Rule not marked as being applied: %s" % rule)
        with self._lists_lock:
            self._being_applied.remove(rule)
            self._applied.append(rule)

    def reset(self):
        """
        Resets all data previously marked as applied and those marked as currently being applied.

        Thread-safe.
        """
        with self._lists_lock:
            del self._being_applied[:]
            del self._applied[:]

            while not self._not_applied.empty():
                self._not_applied.get()
            for rule in self._rules:
                self._not_applied.put(rule)


class RuleSource(ResourceAccessorContainerRegisteringDataSource):
    """
    Rule source where rules are registered from within Python modules within a given directory. These modules can be
    changed on-the-fly.
    """
    # Regex used to determine if a file contains a rule(s)
    FILE_PATH_MATCH_REGEX = ".*rule\.py"
    _COMPILED_FILE_PATH_MATCH_REGEX = re.compile(FILE_PATH_MATCH_REGEX)

    def __init__(self, directory_location: str, resource_accessor: ResourceAccessor=None):
        """
        Constructor.
        :param directory_location: the directory in which rules can be sourced from
        :param resource_accessor: resource accessor that rules will be able to use to access resources
        """
        super().__init__(directory_location, Rule, resource_accessor)

    def is_data_file(self, file_path: str) -> bool:
        return RuleSource._COMPILED_FILE_PATH_MATCH_REGEX.search(file_path)
