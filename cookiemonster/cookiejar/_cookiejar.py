'''
Cookie Jar Abstract Class
=========================
A Cookie Jar acts both as a repository for file metadata while also
maintaining a processing queue. That is, when new/updated metadata is
retrieved for a file, that file then becomes eligible for
(re)processing. This cycle is performed until the file is ultimately
marked as completed (although any later enrichment would again push it
back into the processing queue)

Exportable Classes: `CookieJar`

CookieJar
---------
`CookieJar` implements the abstract base class (interface) for Cookie
Jars. Such implementations must define the following methods:

* `enrich_metadata` should update/append provided metadata to the
  repository for the specified file. If a change is detected, then said
  file should be queued for processing (if it isn't already)

* `mark_as_failed` should mark a file as having failed processing. This
  should have the effect of requeueing the file after a specified grace
  period

* `mark_as_complete` should mark a file as having completed its
  processing successfully

* `mark_as_reprocess` should mark a file as requiring reprocessing,
  which returns it to the queue immediately. Note that this method is
  intended to be invoked under exceptional circumstances (e.g.,
  manually, via some external service, or when downstream processes
  change, etc.) rather than part of the usual workflow
  (i.e., `enrich_metadata` will trigger queueing automatically)

* `get_next_for_processing` should return the next file from the queue
  for processing. If said file has already been processed previously,
  the state of the metadata then used is also provided, so downstream
  processing can detect changes. When returning said next file, the
  state of the processing queue should be updated appropriately

* `queue_length` should return the number of files currently in the
  queue for processing

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015 Genome Research Limited
'''

from abc import ABCMeta, abstractmethod
from datetime import timedelta
from typing import Optional

from hgicommon.listenable import Listenable

from cookiemonster.common.models import CookieCrumbs, CookieProcessState


class CookieJar(Listenable, metaclass=ABCMeta):
    '''
    Interface for an enrichable repository of metadata for files with an
    intrinsic processing queue, where new metadata implies reprocessing
    '''
    @abstractmethod
    def enrich_metadata(self, path: str, metadata: CookieCrumbs):
        '''
        Append/update metadata for a given file, thus changing its state
        and putting it back on the queue (or adding it, if its new)

        @param  path      File path
        @param  metadata  Metadata
        '''
        pass

    @abstractmethod
    def mark_as_failed(self, path: str, requeue_delay: timedelta):
        '''
        Mark a file as having failed processing, thus returning it to
        the queue after a specified period

        @param  path           File path
        @param  requeue_delay  Time to wait before requeuing
        '''
        pass

    @abstractmethod
    def mark_as_complete(self, path: str):
        '''
        Mark a file as having completed processing

        @param  path  File path
        '''
        pass

    @abstractmethod
    def mark_as_reprocess(self, path: str):
        '''
        Mark a file for reprocessing, regardless of changes to its
        metadata, returning it to the queue immediately

        @param  path  File path
        '''
        pass

    @abstractmethod
    def get_next_for_processing(self) -> Optional[CookieProcessState]:
        '''
        Get the next Cookie for processing and update its queue state

        @return The next CookieProcessState for processing (None, if the
                queue is empty)
        @note   This method is thread-safe
        '''
        pass

    @abstractmethod
    def queue_length(self) -> int:
        '''
        Get the number of items ready for processing

        @return Number of items in the queue
        '''
        pass
