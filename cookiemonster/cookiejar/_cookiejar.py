"""
Cookie Jar Abstract Class
=========================
A Cookie Jar acts both as a repository for file metadata while also
maintaining a processing queue. That is, when new/updated metadata is
retrieved for a file, that file then becomes eligible for
(re)processing. This cycle is performed until the file is ultimately
marked as completed (although any later enrichment would again push it
back into the processing queue).

Exportable Classes: `CookieJar`

CookieJar
---------
`CookieJar` implements the abstract base class (interface) for Cookie
Jars. Such implementations must define the following methods:

* `enrich_cookie` should update/append provided metadata (and its
  source) to the repository for the specified file. If a change is
  detected, then said file should be queued for processing (if it isn't
  already). This method should notify its listeners of the updated queue
  length.

* `mark_as_failed` should mark a file as having failed processing. This
  should have the effect of requeueing the file after a specified grace
  period, whereupon listeners should be notified of the updated queue
  length

* `mark_as_complete` should mark a file as having completed its
  processing successfully

* `mark_for_processing` should mark a file as requiring (re)processing,
  which returns it to the queue immediately. Note that this method is
  intended to be invoked under exceptional circumstances (e.g.,
  manually, via some external service, or when downstream processes
  change, etc.) rather than part of the usual workflow (i.e.,
  `enrich_cookie` will trigger queueing automatically). This method
  should notify its listeners of the updated queue length.

* `get_next_for_processing` should return the next Cookie from the queue
  for processing. When returning said next file, the state of the
  processing queue should be updated appropriately

* `queue_length` should return the number of files currently in the
  queue for processing

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015, 2016 Genome Research Limited
"""

from abc import ABCMeta, abstractmethod
from datetime import timedelta
from typing import Optional

from hgicommon.mixable import Listenable

from cookiemonster.common.models import Enrichment, Cookie


_QueueLengthT = int


class CookieJar(Listenable[_QueueLengthT], metaclass=ABCMeta):
    """
    Interface for an enrichable repository of metadata for files with an
    intrinsic processing queue, where new metadata implies reprocessing
    """
    @abstractmethod
    def enrich_cookie(self, identifier: str, enrichment: Enrichment):
        """
        Append/update metadata for a given file, thus changing its state
        and putting it back on the queue (or adding it, if its new),
        with the supplied enrichment

        @param  identifier        Cookie identifier
        @param  enrichment  Enrichment
        """

    @abstractmethod
    def mark_as_failed(self, identifier: str, requeue_delay: timedelta):
        """
        Mark a file as having failed processing, thus returning it to
        the queue after a specified period

        @param  identifier           Cookie identifier
        @param  requeue_delay  Time to wait before requeuing
        """

    @abstractmethod
    def mark_as_complete(self, identifier: str):
        """
        Mark a file as having completed processing and thus removing it
        from the queue

        @param  identifier  Cookie identifier
        """

    @abstractmethod
    def mark_for_processing(self, identifier: str):
        """
        Mark a file for reprocessing, regardless of changes to its
        metadata, returning it to the queue immediately

        @param  identifier  Cookie identifier
        """

    @abstractmethod
    def get_next_for_processing(self) -> Optional[Cookie]:
        """
        Get the next Cookie for processing and update its queue state

        @return The next Cookie for processing (None, if the queue is
                empty)
        @note   This method is thread-safe
        """

    @abstractmethod
    def queue_length(self) -> int:
        """
        Get the number of items ready for processing

        @return Number of items in the queue
        """
