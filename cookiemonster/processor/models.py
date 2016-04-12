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
from typing import Callable, Iterable, Optional

from hgicommon.mixable import Priority
from hgicommon.models import Model

from cookiemonster.common.models import Notification, Cookie, Enrichment
from cookiemonster.common.resource_accessor import ResourceAccessorContainer, ResourceAccessor


class Rule(ResourceAccessorContainer, Priority):
    """
    A production rule that defines a sensory precondition and an action to be executed if the precondition is matched.
    """
    def __init__(self, precondition: Callable[[Cookie, ResourceAccessor], bool],
                 action: Callable[[Cookie, ResourceAccessor], Optional[bool]],
                 priority: int = Priority.MIN_PRIORITY, name=None):
        """
        Constructor.
        :param precondition: the precondition that should return `True` if the action is to be executed
        :param action: the action to be executed if the production rule is triggered. May return whether the system
        should not process any further rules (`True` halts, defaults to `False`)
        :param priority: the priority of the rule (defaults to the minimum priority)
        :param name: optional human readable name to allow identification of the enrichment loader
        """
        super().__init__(priority)
        self._precondition = precondition
        self._action = action
        self.name = name

    def matches(self, cookie: Cookie) -> bool:
        """
        Returns whether this rule applies to the given cookie that is being processed.
        :param cookie: the cookie to check if the rule applies to
        :return: whether the rule applies
        """
        return self._precondition(cookie, self.resource_accessor)

    def execute_action(self, cookie: Cookie) -> bool:
        """
        Executes the action associated to this rule.

        Does not check if this rule's precondition is satisfied.
        :param cookie: the cookie to generate an action for
        :return: whether rule processing should halt and stop processing any more rules
        """
        halt = self._action(cookie, self.resource_accessor)
        if halt is None:
            halt = False
        return  halt

    def __hash__(self):
        return id(self)

    def __str__(self):
        return str(id(self))


class EnrichmentLoader(ResourceAccessorContainer, Priority):
    """
    Data loader that can load specific data that can be used to "enrich" a cookie with more information.
    """
    def __init__(self, can_enrich: Callable[[Cookie, ResourceAccessor], bool],
                 load_enrichment: Callable[[Cookie, ResourceAccessor], Enrichment],
                 priority: int=Priority.MIN_PRIORITY, name=None):
        """
        Constructor.
        :param can_enrich: see `EnrichmentLoader.can_enrich`
        :param load_enrichment: see `EnrichmentLoader.load_enrichment`
        :param priority: the priority used to decide when the enrichment loader should be used
        :param name: optional human readable name to allow identification of the enrichment loader
        """
        super().__init__(priority)
        self._can_enrich = can_enrich
        self._load_enrichment = load_enrichment
        self.name = name

    def can_enrich(self, cookie: Cookie) -> bool:
        """
        Returns whether or not the data that this enrichment loader can enrich the given cookie.
        :param cookie: cookie containing the data that is already known
        :return: whether it is possible to enrich the given cookie
        """
        return self._can_enrich(cookie, self.resource_accessor)

    def load_enrichment(self, cookie: Cookie) -> Enrichment:
        """
        Load data that can be added to a set of known data.
        :param cookie: the pre-existing set of known data
        :return: the loaded data
        """
        return self._load_enrichment(cookie, self.resource_accessor)
