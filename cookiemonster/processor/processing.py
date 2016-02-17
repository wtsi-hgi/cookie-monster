from abc import ABCMeta
from abc import abstractmethod
from typing import Optional, Callable, Iterable

from cookiemonster.common.models import Notification, Cookie
from cookiemonster.processor.models import Rule

ABOUT_NO_RULES_MATCH = "unknown"


class Processor(metaclass=ABCMeta):
    """
    Processor for a single file update.
    """
    @abstractmethod
    def process(self, cookie: Cookie, rules: Iterable[Rule],
                on_complete: Callable[[bool, Optional[Iterable[Notification]]], None]):
        """
        Processes the given cookie.
        :param cookie: the cookie that is to be processed
        :param rules: the processor to use when processing the cookie
        :param on_complete: called when the processing has completed. First argument indicates if at least one rule was
        matched and the second is a set of notifications that were generated.
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
