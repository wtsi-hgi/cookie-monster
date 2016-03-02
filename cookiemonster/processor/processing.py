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
        TODO
        :param cookie:
        :return:
        """

    @abstractmethod
    def execute_rule_actions(self, rule_actions: Iterable[RuleAction]):
        """
        TODO
        :param rule_actions:
        """

    @abstractmethod
    def handle_cookie_enrichment(self, cookie: Cookie):
        """
        TODO
        :param cookie:
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
