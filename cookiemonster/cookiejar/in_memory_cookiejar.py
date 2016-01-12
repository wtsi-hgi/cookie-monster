from datetime import timedelta
from multiprocessing import Lock
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
        self._lists_lock = Lock()

    def enrich_cookie(self, path: str, enrichment: Enrichment):
        with self._lists_lock:
            if path not in self._known_data:
                self._known_data[path] = Cookie(path)

        self._known_data[path].enrichments.append(enrichment)
        self.mark_for_processing(path)

    def mark_as_failed(self, path: str, requeue_delay: timedelta):
        if path not in self._known_data:
            raise ValueError("File not known: %s" % path)
        with self._lists_lock:
            self._assert_is_being_processed(path)
            self._processing.remove(path)
            self._failed.append(path)

    def mark_as_complete(self, path: str):
        if path not in self._known_data:
            raise ValueError("File not known: %s" % path)
        with self._lists_lock:
            self._assert_is_being_processed(path)
            self._processing.remove(path)
            self._completed.append(path)

    def mark_for_processing(self, path: str):
        if path not in self._known_data:
            self._known_data[path] = Cookie(path)

        with self._lists_lock:
            if path in self._completed:
                self._completed.remove(path)
            self._waiting.append(path)
        self.notify_listeners(self.queue_length())

    def get_next_for_processing(self) -> Optional[Cookie]:
        with self._lists_lock:
            if len(self._waiting) == 0:
                return None
            path = self._waiting.pop()
            self._processing.append(path)
            self._assert_is_being_processed(path)
        return self._known_data[path]

    def queue_length(self) -> int:
        return len(self._waiting)

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
