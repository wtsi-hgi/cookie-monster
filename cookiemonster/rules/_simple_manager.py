from typing import Any, List, Optional

from cookiemonster.common.models import Notification, CookieProcessState
from cookiemonster.cookiejar import CookieJar
from cookiemonster.rules._rules_management import RulesManager
from cookiemonster.rules._simple_processor import SimpleProcessor
from cookiemonster.rules.manager import ProcessorManager
from cookiemonster.rules.processor import Processor


class SimpleProcessorManager(ProcessorManager):
    """
    Simple implementation to of managing the continuous processing of file updates.
    """
    def __init__(self, number_of_processors: int, cookie_jar: CookieJar, rules_manager: RulesManager, notifier: Any):
        """
        Default constructor.
        :param number_of_processors:
        :param cookie_jar:
        :param rules_manager:
        :param notifier:
        """
        self._data_manager = cookie_jar
        self._rules_manager = rules_manager
        self._notifier = notifier

        self._idle_processors = []
        self._busy_processors = []

        for i in range(number_of_processors):
            processor = SimpleProcessor()
            self._idle_processors.append(processor)

    def process_any_jobs(self):
        processor = self._claim_processor()

        if processor is not None:
            job = self._data_manager.get_next_for_processing()

            if job is not None:
                def on_complete(notifications: List[Notification]):
                    self._busy_processors.remove(processor)
                    self.on_processed(job, notifications)
                    self.process_any_jobs()

                processor.process(job, self._rules_manager.get_rules(), on_complete)

    def on_processed(self, job: CookieProcessState, notifications: List[Notification]):
        job_id = job.path

        if len(notifications) == 0:
            self._data_manager.mark_as_failed(job_id)   # TODO: Correct method call?
            raise NotImplementedError()    # TODO: Load additional data
        else:
            for notification in notifications:
                self._notifier.add(notification)
            self._data_manager.mark_as_complete(job_id)

    def _claim_processor(self) -> Optional[Processor]:
        """
        TODO: Make synchronous
        :return:
        """
        if len(self._idle_processors) == 0:
            return None
        return self._idle_processors.pop()

