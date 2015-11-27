from threading import Lock, Thread
from typing import Any, List, Optional

from cookiemonster.common.models import Notification, CookieProcessState
from cookiemonster.cookiejar import CookieJar
from cookiemonster.rules._rules_management import RulesManager
from cookiemonster.rules._simple_processor import SimpleProcessor
from cookiemonster.rules.manager import ProcessorManager
from cookiemonster.rules.processor import Processor


class SimpleProcessorManager(ProcessorManager):
    """
    Simple implementation of managing the continuous processing of new data.
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

        self._idle_processors = set()
        self._busy_processors = set()
        self._lists_lock = Lock()

        for i in range(number_of_processors):
            processor = SimpleProcessor()
            self._idle_processors.add(processor)

    def process_any_jobs(self):
        processor = self._claim_processor()

        if processor is not None:
            job = self._data_manager.get_next_for_processing()

            if job is not None:
                def on_complete(rules_matched: bool, notifications: List[Notification]):
                    self.on_job_processed(job, rules_matched, notifications)
                    self._release_processor(processor)
                    self.process_any_jobs()

                Thread(target=processor.process, args=(job, self._rules_manager.get_rules(), on_complete)).start()
            else:
                self._release_processor(processor)

    def on_job_processed(
            self, job: CookieProcessState, rules_matched: bool, notifications: List[Notification]=()):
        job_id = job.current_state.path

        if rules_matched:
            for notification in notifications:
                self._notifier.add(notification)
            self._data_manager.mark_as_complete(job_id)
        else:
            self._data_manager.mark_as_failed(job_id)   # TODO: Correct method call?
            raise NotImplementedError()    # TODO: Load additional data

    def _claim_processor(self) -> Optional[Processor]:
        """
        Claims a processor.

        Thread-safe.
        :return: the claimed processor, else `None` if no were available
        """
        if len(self._idle_processors) == 0:
            return None
        self._lists_lock.acquire()
        processor = self._idle_processors.pop()
        self._busy_processors.add(processor)
        self._lists_lock.release()
        return processor

    def _release_processor(self, processor: Processor):
        """
        Releases a processor.

        Thread-safe.
        :param processor: the processor that is currently in use and should be released
        """
        assert processor in self._busy_processors
        assert processor not in self._idle_processors
        self._lists_lock.acquire()
        self._busy_processors.remove(processor)
        self._idle_processors.add(processor)
        self._lists_lock.release()
