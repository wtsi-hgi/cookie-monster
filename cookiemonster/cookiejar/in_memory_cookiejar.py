from datetime import timedelta
from multiprocessing import Lock
from typing import Optional

from cookiemonster.common.models import Cookie, Enrichment
from cookiemonster.cookiejar import CookieJar


class InMemoryCookieJar(CookieJar):
    """
    In memory implementation of a `CookieJar`.
    """
    def __init__(self):
        super(InMemoryCookieJar, self).__init__()
        self._known_data = dict()   # type: Dict[str, Cookie]
        self._processing = []   # type: List[str]
        self._waiting = []  # type: List[str]
        self._failed = []   # type: List[str]
        self._completed = []    # type: List[str]
        self.lists_lock = Lock()

    def enrich_cookie(self, path: str, enrichment: Enrichment):
        if path not in self._known_data:
            cookie = Cookie(path)
            self._known_data[path] = cookie

        self._known_data[path].enrichments += enrichment

        self.lists_lock.acquire()
        if path not in self._waiting:
            self._waiting.append(path)
            self.notify_listeners(None)     # FIXME: Should not be forced to give `None`
        self.lists_lock.release()

    def mark_as_failed(self, path: str, requeue_delay: timedelta):
        if path not in self._known_data:
            raise ValueError("File not known: " % path)
        self.lists_lock.acquire()
        self._assert_was_being_processed(path)
        self._processing.remove(path)
        self._failed.append(path)
        self.lists_lock.release()

    def mark_as_complete(self, path: str):
        if path not in self._known_data:
            raise ValueError("File not known: " % path)
        self.lists_lock.acquire()
        self._assert_was_being_processed(path)
        self._processing.remove(path)
        self._completed.append(path)
        self.lists_lock.release()

    def mark_as_reprocess(self, path: str):
        if path not in self._known_data:
            raise ValueError("File not known: " % path)
        self.lists_lock.acquire()
        self._assert_was_being_processed(path)
        self._processing.remove(path)
        self._waiting.append(path)
        self.lists_lock.release()
        self.notify_listeners(None)     # FIXME: Should not be forced to give `None`

    def get_next_for_processing(self) -> Optional[Cookie]:
        if len(self._waiting) == 0:
            return None
        self.lists_lock.acquire()
        path = self._waiting.pop()
        self._processing.append(path)
        self._assert_was_being_processed()
        self.lists_lock.release()
        return self._known_data[path]

    def queue_length(self) -> int:
        return len(self._waiting)

    def _assert_was_being_processed(self, path: str):
        """
        Asserts that a file, identified by its path, was being processed.
        :param path: the file's identifier
        """
        assert path in self._known_data
        assert path in self._processing
        assert path not in self._completed
        assert path not in self._failed
        assert path not in self._waiting
