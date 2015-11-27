import copy
from datetime import timedelta
from typing import Optional

from typing import List, Dict

from cookiemonster.common.models import Cookie
from cookiemonster.common.models import CookieCrumbs, CookieProcessState
from cookiemonster.cookiejar import CookieJar


class InMemoryCookieJar(CookieJar):
    """
    In memory implementation of a `CookieJar`.
    """
    def __init__(self):
        super(InMemoryCookieJar, self).__init__()
        self._known_data = dict()   # type: Dict[str, CookieProcessState]
        self._processing = []   # type: List[str]
        self._waiting = []  # type: List[str]
        self._failed = []   # type: List[str]
        self._completed = []    # type: List[str]

    def enrich_metadata(self, path: str, metadata: CookieCrumbs):
        if path not in self._known_data:
            current_state = Cookie(path)
            current_state.metadata = metadata
            self._known_data[path] = CookieProcessState(current_state)
        else:
            known = self._known_data[path]
            known.processed_state = known.current_state
            known.current_state = copy.deepcopy(known.processed_state)

            for key, value in metadata.items():
                known.current_state.metadata[key] = value

    def mark_as_failed(self, path: str, requeue_delay: timedelta):
        if path not in self._known_data:
            raise ValueError("File not known: " % path)
        self._assert_was_being_processed(path)
        self._processing.remove(path)
        self._failed.append(path)

    def mark_as_complete(self, path: str):
        if path not in self._known_data:
            raise ValueError("File not known: " % path)
        self._assert_was_being_processed(path)
        self._processing.remove(path)
        self._completed.append(path)

    def mark_as_reprocess(self, path: str):
        if path not in self._known_data:
            raise ValueError("File not known: " % path)
        self._assert_was_being_processed(path)
        self._processing.remove(path)
        self._waiting.append(path)

    def get_next_for_processing(self) -> Optional[CookieProcessState]:
        path = self._waiting.pop()
        self._processing.append(path)
        return self._known_data[path]

    def queue_length(self) -> int:
        return len(self._waiting)

    def _assert_was_being_processed(self, path: str):
        """
        Asserts that a file, identified by its path, was being processed.
        :param path: the file's identifier
        """
        assert path in self._processing
        assert path not in self._completed
        assert path not in self._failed
        assert path not in self._waiting
