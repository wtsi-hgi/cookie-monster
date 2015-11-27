from abc import ABCMeta
from abc import abstractmethod
from typing import List, Optional

from cookiemonster.common.models import Notification, CookieProcessState


class ProcessorManager(metaclass=ABCMeta):
    """
    Manages the continuous processing of file updates.
    """
    @abstractmethod
    def process_any_jobs(self):
        """
        Check for new jobs that are to be processed and proceses them if they are available.
        """
        pass

    @abstractmethod
    def on_job_processed(
            self, job: CookieProcessState, rules_matched: bool, notifications: List[Notification]=()):
        """
        Called when processing of a job has been completed
        :param job: the job that has been processed
        :param rules_matched: whether at least one rule was matched during the processing
        :param notifications: list of external processes that are to be notified. List should only be givne if processor
        were matched
        """
        pass
