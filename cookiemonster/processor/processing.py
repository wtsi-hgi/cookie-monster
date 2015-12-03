from abc import ABCMeta
from abc import abstractmethod
from queue import PriorityQueue, Queue
from threading import Lock
from typing import List, Optional, Callable, Iterable, Sequence
from cookiemonster.common.models import Notification, Cookie
from cookiemonster.processor._models import Rule


class Processor(metaclass=ABCMeta):
    """
    Processor for a single file update.
    """
    @abstractmethod
    def process(self, cookie: Cookie, rules: Iterable[Rule],
                on_complete: Callable[[bool, Optional[List[Notification]]], None]):
        """
        Processes the given cookie.
        :param cookie: the cookie that is to be processed
        :param rules: the processor to use when processing the cookie
        :param on_complete: called when the processing has completed. First argument indicates if at least one rule was
        matched and the second is a set of notifications that were generated.
        """
        pass


class ProcessorManager(metaclass=ABCMeta):
    """
    Manages the continuous processing of file updates.
    """
    @abstractmethod
    def process_any_cookies(self):
        """
        Check for new cookie jobs that are to be processed and process them if they are available.
        """
        pass

    @abstractmethod
    def on_cookie_processed(self, cookie: Cookie, stop_processing: bool, notifications: List[Notification]=()):
        """
        Called when processing of a cookie has been completed
        :param cookie: the cookie that has been processed
        :param stop_processing: whether rule indicates that we should stop processing the given cookie
        :param notifications: list of external processes that are to be notified. List should only be givne if processor
        were matched
        """
        pass
