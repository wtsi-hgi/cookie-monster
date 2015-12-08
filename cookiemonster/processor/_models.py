from typing import Callable, Iterable

from hgicommon.mixable import Priority
from hgicommon.models import Model

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


class Rule(Model, Priority):
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

    def __eq__(self, other):
        return id(self) == id(other)

    def __hash__(self):
        return id(self)

    def __str__(self):
        return str(id(self))

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


class EnrichmentLoader(Model, Priority):
    """
    Data loader that can load specific data that can be used to "enrich" a cookie with more information.
    """
    def __init__(self, can_enrich: Callable[[Cookie], bool], load_enrichment: Callable[[Cookie], Enrichment],
                 priority: int=Priority.MIN_PRIORITY):
        """
        Default constructor.
        :param can_enrich: see `EnrichmentLoader.can_enrich`
        :param load_enrichment: see `EnrichmentLoader.load_enrichment`
        :param priority: the priority used to decide when the enrichment loader should be used
        """
        super().__init__(priority)
        self._can_enrich = can_enrich
        self._load_enrichment = load_enrichment

    def can_enrich(self, cookie: Cookie) -> bool:
        """
        Returns whether or not the data that this enrichment loader can enrich the given cookie
        :param cookie: cookie containing the data that is already known
        :return: whether it is possible to enrich the given cookie
        """
        return self._can_enrich(cookie)

    def load_enrichment(self, cookie: Cookie) -> Enrichment:
        """
        Load data that can be added to a set of known data.
        :param cookie: the pre-existing set of known data
        :return: the loaded data
        """
        return self._load_enrichment(cookie)
