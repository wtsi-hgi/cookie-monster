import time
from collections import defaultdict
from datetime import datetime, timedelta
from multiprocessing import Lock
from threading import Timer
from typing import Optional, List, Dict

from cookiemonster.common.models import Cookie, Enrichment
from cookiemonster.cookiejar import CookieJar


class InMemoryCookieJar(CookieJar):
    """
    In memory implementation of a `CookieJar`.
    """
    def __init__(self):
        """
        Constructor.
        """
        super().__init__()
        self._known_data = dict()   # type: Dict[str, Cookie]
        self._processing = []   # type: List[str]
        self._waiting = []  # type: List[str]
        self._failed = []   # type: List[str]
        self._completed = []    # type: List[str]
        self._reprocess_on_complete = []    # type: List[str]
        self._lists_lock = Lock()
        self._timers = defaultdict(list)   # type: Dict[int, List[Timer]]

    def enrich_cookie(self, path: str, enrichment: Enrichment):
        with self._lists_lock:
            if path not in self._known_data:
                self._known_data[path] = Cookie(path)

        self._known_data[path].enrichments.append(enrichment)
        self.mark_for_processing(path)

    def mark_as_failed(self, path: str, requeue_delay: timedelta=None):
        if path not in self._known_data:
            raise ValueError("Not known: %s" % path)
        with self._lists_lock:
            self._assert_is_being_processed(path)
            self._processing.remove(path)
            self._failed.append(path)

        if requeue_delay is not None:
            end_time = self._get_time() + requeue_delay.total_seconds()

            def on_delay_end():
                if timer in self._timers[end_time]:
                    self._timers[end_time].remove(timer)
                    self._reprocess(path)

            timer = Timer(requeue_delay.total_seconds(), on_delay_end)
            self._timers[end_time].append(timer)
            timer.start()
        else:
            self._on_complete(path)

    def mark_as_complete(self, path: str):
        if path not in self._known_data:
            raise ValueError("Not known: %s" % path)
        with self._lists_lock:
            self._assert_is_being_processed(path)
            self._processing.remove(path)
            self._completed.append(path)
        self._on_complete(path)

    def mark_for_processing(self, path: str):
        if path not in self._known_data:
            self._known_data[path] = Cookie(path)

        notify = True
        with self._lists_lock:
            if path in self._completed:
                self._completed.remove(path)
            if path in self._processing:
                if path not in self._reprocess_on_complete:
                    self._reprocess_on_complete.append(path)
                notify = False
            elif path not in self._waiting:
                self._waiting.append(path)

        if notify:
            self.notify_listeners(self.queue_length())

    def get_next_for_processing(self) -> Optional[Cookie]:
        with self._lists_lock:
            if len(self._waiting) == 0:
                return None
            path = self._waiting.pop(0)
            self._processing.append(path)
            self._assert_is_being_processed(path)
        return self._known_data[path]

    def queue_length(self) -> int:
        return len(self._waiting)

    def _get_time(self) -> int:
        """
        Gets the current monotonic time.
        :return: current time
        """
        return time.monotonic()

    def _reprocess(self, path: str):
        """
        Reprocess Cookie with the given path where processing has previously failed.
        :param path: path of cookie to reprocess
        """
        with self._lists_lock:
            self._failed.remove(path)
        self.mark_for_processing(path)

    def _on_complete(self, path: str):
        reprocess = False
        with self._lists_lock:
            if path in self._reprocess_on_complete:
                self._reprocess_on_complete.remove(path)
                reprocess = True
        if reprocess:
            self.mark_for_processing(path)

    def _assert_is_being_processed(self, path: str):
        """
        Asserts that a file, identified by its path, was being processed.
        :param path: the file's identifier
        """
        assert path in self._known_data
        assert path in self._processing
        assert path not in self._completed
        assert path not in self._failed
        assert path not in self._waiting
