"""
Legalese
--------
Copyright (c) 2015, 2016 Genome Research Ltd.

Authors:
* Colin Nolan <cn13@sanger.ac.uk>
* Christopher Harrison <ch12@sanger.ac.uk>

This file is part of Cookie Monster.

Cookie Monster is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by the
Free Software Foundation; either version 3 of the License, or (at your
option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License along
with this program. If not, see <http://www.gnu.org/licenses/>.
"""
import time
from collections import defaultdict
from datetime import timedelta
from multiprocessing import Lock
from threading import Timer
from typing import Any, Optional, List, Dict

from cookiemonster.common.models import Cookie, Enrichment
from cookiemonster.cookiejar import CookieJar


def _remove_if_exists(lst:List, el:Any):
    """ Remove first appearance of el from lst, if exists """
    if el in lst:
        lst.remove(el)


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
        self._delete_on_complete = []    # type: List[str]
        self._lists_lock = Lock()
        self._timers = defaultdict(list)   # type: Dict[int, List[Timer]]

    def fetch_cookie(self, identifier: str) -> Optional[Cookie]:
        with self._lists_lock:
            return self._known_data.get(identifier, None)

    def delete_cookie(self, identifier: str):
        with self._lists_lock:
            if identifier in self._known_data:
                if identifier in self._processing:
                    self._known_data[identifier].enrichments[:] = []
                    self._delete_on_complete.append(identifier)
                else:
                    self._cleanup(identifier)

    def enrich_cookie(self, identifier: str, enrichment: Enrichment, mark_for_processing: bool=True):
        with self._lists_lock:
            if identifier not in self._known_data:
                self._known_data[identifier] = Cookie(identifier)

        self._known_data[identifier].enrichments.append(enrichment)
        if mark_for_processing:
            self.mark_for_processing(identifier)

    def mark_as_failed(self, identifier: str, requeue_delay: timedelta=timedelta(0)):
        if identifier not in self._known_data:
            raise ValueError("Not known: %s" % identifier)
        with self._lists_lock:
            self._assert_is_being_processed(identifier)
            self._processing.remove(identifier)
            self._failed.append(identifier)

        if requeue_delay is not None:
            if requeue_delay.total_seconds() == 0:
                self._reprocess(identifier)
            else:
                end_time = self._get_time() + requeue_delay.total_seconds()

                def on_delay_end():
                    if timer in self._timers[end_time]:
                        self._timers[end_time].remove(timer)
                        self._reprocess(identifier)

                timer = Timer(requeue_delay.total_seconds(), on_delay_end)
                self._timers[end_time].append(timer)
                timer.start()
        else:
            self._on_complete(identifier)

    def mark_as_complete(self, identifier: str):
        if identifier not in self._known_data:
            raise ValueError("Not known: %s" % identifier)
        with self._lists_lock:
            self._assert_is_being_processed(identifier)
            self._processing.remove(identifier)
            self._completed.append(identifier)
        self._on_complete(identifier)

    def mark_for_processing(self, identifier: str):
        if identifier not in self._known_data:
            self._known_data[identifier] = Cookie(identifier)

        notify = True
        with self._lists_lock:
            if identifier in self._completed:
                self._completed.remove(identifier)
            if identifier in self._processing:
                if identifier not in self._reprocess_on_complete:
                    self._reprocess_on_complete.append(identifier)
                notify = False
            elif identifier not in self._waiting:
                self._waiting.append(identifier)

        if notify:
            self.notify_listeners()

    def get_next_for_processing(self) -> Optional[Cookie]:
        with self._lists_lock:
            if len(self._waiting) == 0:
                return None
            identifier = self._waiting.pop(0)
            self._processing.append(identifier)
            self._assert_is_being_processed(identifier)
        return self._known_data[identifier]

    def queue_length(self) -> int:
        return len(self._waiting)

    def _get_time(self) -> int:
        """
        Gets the current monotonic time.
        :return: current time
        """
        return time.monotonic()

    def _reprocess(self, identifier: str):
        """
        Reprocess Cookie with the given identifier where processing has previously failed.
        :param identifier: identifier of cookie to reprocess
        """
        with self._lists_lock:
            self._failed.remove(identifier)
        self.mark_for_processing(identifier)

    def _cleanup(self, identifier: str):
        """
        Clean up the queue state of a deleted in-progress Cookie
        :param identifier: identifier of cookie to cleanup
        """
        _remove_if_exists(self._processing, identifier)
        _remove_if_exists(self._waiting, identifier)
        _remove_if_exists(self._failed, identifier)
        _remove_if_exists(self._completed, identifier)
        _remove_if_exists(self._reprocess_on_complete, identifier)
        _remove_if_exists(self._delete_on_complete, identifier)
        del self._known_data[identifier]

    def _on_complete(self, identifier: str):
        reprocess = False
        with self._lists_lock:
            if identifier in self._delete_on_complete:
                self._cleanup(identifier)
            if identifier in self._reprocess_on_complete:
                self._reprocess_on_complete.remove(identifier)
                reprocess = True
        if reprocess:
            self.mark_for_processing(identifier)

    def _assert_is_being_processed(self, identifier: str):
        """
        Asserts that a file, identified by its identifier, was being processed.
        :param identifier: the file's identifier
        """
        assert identifier in self._known_data
        assert identifier in self._processing
        assert identifier not in self._completed
        assert identifier not in self._failed
        assert identifier not in self._waiting
