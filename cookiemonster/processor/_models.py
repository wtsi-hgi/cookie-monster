from abc import ABC, ABCMeta
from typing import Callable, Iterable

import sys
from hgicommon.models import Model, Priority
from multiprocessing import Lock

from cookiemonster.common.models import Notification, Cookie, Enrichment


class RuleAction(Model):
    """
    A model of the action that has outcome from matching a rule.
    """
    def __init__(self, notifications: Iterable[Notification], terminate_processing: bool):
        """
        Default constructor.
        :param notifications: notifications for external processes
        :param terminate_processing: whether the data processor should stop processing the update
        """
        self.notifications = notifications
        self.terminate_processing = terminate_processing


class Rule(Priority):
    """
    A model of a rule that defines an action that should be executed if a criteria is matched.
    """
    def __init__(self, matching_criteria: Callable[[Cookie], bool], action_generator: Callable[[Cookie], RuleAction],
                 priority: int = Priority.MIN_PRIORITY):
        """
        Default constructor.
        :param matching_criteria: see `Rule.matching_criteria`
        :param action_generator: see `Rule.action_generator`
        :param priority: the priority of the rule (default to the minimum possible)
        """
        super().__init__(priority)
        self._matching_criteria = matching_criteria
        self._action_generator = action_generator

    def matching_criteria(self, cookie: Cookie) -> bool:
        """
        Returns whether this rule applies to the given cookie that is being processed.
        :param cookie: the cookie to check if the rule applies to
        :return: whether the rule applies
        """
        return self._matching_criteria(cookie)

    def action_generator(self, cookie: Cookie) -> RuleAction:
        """
        Returns the action that should be taken in response to the given cookie.

        Will raise a `ValueError` if the rule does not match the given cookie
        :param cookie: the cookie to generate an action for
        :return: the generated action
        """
        if not self.matching_criteria(cookie):
            return ValueError("Rules does not match cookie: %s" % cookie)
        return self._action_generator(cookie)

    def __eq__(self, other):
        return id(self) == id(other)

    def __hash__(self):
        return id(self)

    def __str__(self):
        return str(id(self))


class EnrichmentLoader(Model):
    """
    TODO
    """
    def __init__(self, is_already_known: Callable[[Cookie], bool], load: Callable[[Cookie], Enrichment]):
        """
        Default constructor.
        :param is_already_known: see `EnrichmentLoader.is_loaded`
        :param load: see `EnrichmentLoader.load`
        """
        self._is_already_known = is_already_known
        self._load = load

    def is_loaded(self, cookie: Cookie) -> bool:
        """
        Returns whether or not the data that this enrichment loader can load is already in the given cookie.
        :param cookie: cookie containing the data that is already known
        :return: whether the data is already in the cookie
        """
        return self._is_already_known(cookie)

    def load(self, cookie: Cookie) -> Enrichment:
        """
        Load data that can be added to a set of known data.
        :param cookie: the pre-existing set of known data
        :return: the loaded data
        """
        return self._load(cookie)
