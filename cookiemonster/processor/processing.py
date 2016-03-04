from abc import ABCMeta
from abc import abstractmethod
from typing import Iterable, Sequence

from cookiemonster.common.models import Cookie
from cookiemonster.processor.models import RuleAction

ABOUT_NO_RULES_MATCH = "no rules matched"


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
        rule_actions = self.evaluate_rules_with_cookie(cookie)

        # Execute rule actions
        self.execute_rule_actions(rule_actions)

        if True not in [rule_action.terminate_processing for rule_action in rule_actions]:
            # Enrich Cookie further
            self.handle_cookie_enrichment(cookie)

    @abstractmethod
    def evaluate_rules_with_cookie(self, cookie: Cookie) -> Sequence[RuleAction]:
        """
        Evaluates the rules known by this processor with the given Cookie. Rules should be evaluated in order of
        priority and evaluation should stop if a rule production signals no further processing is required. Rules must
        not be allowed make changes to the Cookie.
        :param cookie: the cookie to evaluate rules against
        :return: the rule actions produced by rule matches
        """

    @abstractmethod
    def execute_rule_actions(self, rule_actions: Iterable[RuleAction]):
        """
        Executes the given rule actions produced by matching rules.
        :param rule_actions: the rule actions to execute
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

        Non-blocking.
        """
