"""
Legalese
--------
Copyright (c) 2015, 2016 Genome Research Ltd.

Author: Colin Nolan <cn13@sanger.ac.uk>

This file is part of Cookie Monster.

Cookie Monster is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by the
Free Software Foundation; either version 3 of the License, or (at your
option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License along
with this program. If not, see <http://www.gnu.org/licenses/>.
"""
import re
from copy import copy
from multiprocessing import Lock
from queue import PriorityQueue
from typing import Iterable, Optional

from hgicommon.data_source import RegisteringDataSource

from cookiemonster.common.context import ContextContainerRegisteringDataSource, Context
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
        self._rules = copy(rules)
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


class RuleSource(ContextContainerRegisteringDataSource):
    """
    Rule source where rules are registered from within Python modules within a given directory. These modules can be
    changed on-the-fly.
    """
    # Regex used to determine if a file contains a rule(s)
    FILE_PATH_MATCH_REGEX = ".*rule\.py"
    _COMPILED_FILE_PATH_MATCH_REGEX = re.compile(FILE_PATH_MATCH_REGEX)

    def __init__(self, directory_location: str, context: Context=None):
        """
        Constructor.
        :param directory_location: the directory in which rules can be sourced from
        :param context: the context that rules will be able to access
        """
        super().__init__(directory_location, Rule, context)

    def is_data_file(self, file_path: str) -> bool:
        return RuleSource._COMPILED_FILE_PATH_MATCH_REGEX.search(file_path)
