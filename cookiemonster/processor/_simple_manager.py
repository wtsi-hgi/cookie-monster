from threading import Lock, Thread
from typing import Any, List, Optional

from cookiemonster.common.models import Notification, CookieProcessState
from cookiemonster.cookiejar import CookieJar
from cookiemonster.notifier.notifier import Notifier
from cookiemonster.processor._data_management import DataManager
from cookiemonster.processor._rules_management import RulesManager
from cookiemonster.processor._simple_processor import SimpleProcessor
from cookiemonster.processor.manager import ProcessorManager
from cookiemonster.processor.processor import Processor


class SimpleProcessorManager(ProcessorManager):
    """
    Simple implementation of managing the continuous processing of new data.
    """
    def __init__(self, number_of_processors: int, cookie_jar: CookieJar, rules_manager: RulesManager,
                 data_manager: DataManager, notifier: Notifier):
        """
        TODO
        :param number_of_processors:
        :param cookie_jar:
        :param rules_manager:
        :param data_manager:
        :param notifier:
        :return:
        """
        self._cookie_jar = cookie_jar
        self._rules_manager = rules_manager
        self._notifier = notifier
        self._data_manager = data_manager

        self._idle_processors = set()
        self._busy_processors = set()
        self._lists_lock = Lock()

        for i in range(number_of_processors):
            processor = SimpleProcessor()
            self._idle_processors.add(processor)

    def process_any_jobs(self):
        processor = self._claim_processor()

        if processor is not None:
            job = self._cookie_jar.get_next_for_processing()

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
        if rules_matched:
            for notification in notifications:
                self._notifier.do(notification)
            self._cookie_jar.mark_as_complete(job.path)
        else:
            next_data = self._data_manager.load_next(job.current_state)

            if next_data is None:
                # FIXME: No guarantee that such a notification can be given
                self._notifier.do(Notification("unknown", job.path))
                self._cookie_jar.mark_as_complete(job.path)
            else:
                self._cookie_jar.enrich_metadata(job.path, next_data)
                self._cookie_jar.mark_as_reprocess(job.path)

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
