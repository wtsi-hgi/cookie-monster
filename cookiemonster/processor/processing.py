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
from abc import ABCMeta
from abc import abstractmethod

from cookiemonster.common.models import Cookie


class Processor(metaclass=ABCMeta):
    """
    Processor for a single Cookie.
    """
    def process_cookie(self, cookie: Cookie):
        """
        Processes the given Cookie.
        :param cookie: the Cookie to process
        """
        # Evaluate rules to get rule actions
        halt = self.evaluate_rules_with_cookie(cookie)

        if not halt:
            # Enrich Cookie further
            self.handle_cookie_enrichment(cookie)

    @abstractmethod
    def evaluate_rules_with_cookie(self, cookie: Cookie) -> bool:
        """
        Evaluates the rules known by this processor with the given Cookie. Rules should be evaluated in order of
        priority and evaluation should stop if a rule action signals no further processing is required via a `True`
        return value. Rules must not be allowed make changes to the Cookie.
        :param cookie: the cookie to evaluate rules against
        :return: whether the system should stop evaluating further rules
        """

    @abstractmethod
    def handle_cookie_enrichment(self, cookie: Cookie):
        """
        Handle the enrichment of the given Cookie using the enrichment loaders known by the processor. If it is possible
        to enrich the Cookie, the enrichment should be loaded and the Cookie should be enriched in the knowledge base
        (Cookie Jar). If no enrichments can be loaded, this fact should be broadcast to all notification listeners
        known by this processor.
        :param cookie: the cookie to enrich
        """


class ProcessorManager(metaclass=ABCMeta):
    """
    Manager of the processing of enriched Cookies.
    """
    @abstractmethod
    def process_any_cookies(self):
        """
        Check for Cookies that are to be processed and triggers a `Processor` to process them if required.
        """
