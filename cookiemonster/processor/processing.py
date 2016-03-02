from abc import ABCMeta
from abc import abstractmethod
from typing import Optional, Callable, Iterable, Sequence

from cookiemonster.common.models import Notification, Cookie
from cookiemonster.processor.models import Rule, RuleAction

ABOUT_NO_RULES_MATCH = "no rules matched"


class Processor(metaclass=ABCMeta):
    """
    Processor for a single Cookie - pushes the Cookie through the "rule engine".
    """
    def process_cookie(self, cookie: Cookie):
        """
        Proceses the given Cookie.
        :param cookie: the Cookie to process
        """
        # Evaluate rules
        rule_actions = self.evaluate_rules_with_cookie(cookie)

        # Execute rule actions
        self.execute_rule_actions(rule_actions)

        # Enrich Cookie further if required
        if True not in [rule_action.terminate_processing for rule_action in rule_actions]:
            self.enrich_cookie(cookie)

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
    def enrich_cookie(self, cookie: Cookie):
        """
        TODO
        :param cookie:
        """


class ProcessorManager(metaclass=ABCMeta):
    """
    Manager of the continuous processing of dirty cookies.
    """
    @abstractmethod
    def process_any_cookies(self):
        """
        Check for new cookie jobs that are to be processed and process them if they are available.
        """

    @abstractmethod
    def on_cookie_processed(self, cookie: Cookie, stop_processing: bool, notifications: Iterable[Notification]=()):
        """
        Called when processing of a cookie has been completed.
        :param cookie: the cookie that has been processed
        :param stop_processing: whether rule indicates that we should stop processing the given cookie
        :param notifications: external processes that are to be notified
        """
