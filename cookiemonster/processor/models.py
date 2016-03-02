from typing import Callable, Iterable

from hgicommon.mixable import Priority
from hgicommon.models import Model

from cookiemonster.common.models import Notification, Cookie, Enrichment


class RuleAction(Model):
    """
    A model of the action that has outcome from matching a rule.
    """
    def __init__(self, notifications: Iterable[Notification], terminate_processing: bool=False):
        """
        Constructor.
        :param notifications: notifications for external processes
        :param terminate_processing: whether the data processor should stop processing the update
        """
        self.notifications = notifications
        self.terminate_processing = terminate_processing


class Rule(Model, Priority):
    """
    A model of a rule that defines an action that should be executed if a criteria is matched.
    """
    def __init__(self, matches: Callable[[Cookie], bool], generate_action: Callable[[Cookie], RuleAction],
                 priority: int = Priority.MIN_PRIORITY):
        """
        Default constructor.
        :param matches: see `Rule._matches`
        :param generate_action: see `Rule.generate_action`
        :param priority: the priority of the rule (default to the minimum possible)
        """
        super().__init__(priority)
        self._matches = matches
        self._generate_action = generate_action

    def matches(self, cookie: Cookie) -> bool:
        """
        Returns whether this rule applies to the given cookie that is being processed.
        :param cookie: the cookie to check if the rule applies to
        :return: whether the rule applies
        """
        return self._matches(cookie)

    def generate_action(self, cookie: Cookie) -> RuleAction:
        """
        Returns the action that should be taken in response to the given cookie.

        Will raise a `ValueError` if the rule does not match the given cookie
        :param cookie: the cookie to generate an action for
        :return: the generated action
        """
        if not self._matches(cookie):
            return ValueError("Rules does not match cookie: %s" % cookie)
        return self._generate_action(cookie)

    def __hash__(self):
        return id(self)

    def __str__(self):
        return str(id(self))


class EnrichmentLoader(Model, Priority):
    """
    Data loader that can load specific data that can be used to "enrich" a cookie with more information.
    """
    def __init__(self, can_enrich: Callable[[Cookie], bool], load_enrichment: Callable[[Cookie], Enrichment],
                 priority: int=Priority.MIN_PRIORITY):
        """
        Constructor.
        :param can_enrich: see `EnrichmentLoader.can_enrich`
        :param load_enrichment: see `EnrichmentLoader.load_enrichment`
        :param priority: the priority used to decide when the enrichment loader should be used
        """
        super().__init__(priority)
        self._can_enrich = can_enrich
        self._load_enrichment = load_enrichment

    def can_enrich(self, cookie: Cookie) -> bool:
        """
        Returns whether or not the data that this enrichment loader can enrich the given cookie.
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
