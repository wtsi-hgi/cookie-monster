'''
Cookie Jar
==========

TODO: Redo documentation!!

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015 Genome Research Limited
'''

# TODO Testing code...

# TODO Change this to match new interface requirements
from datetime import timedelta
from typing import Optional, Union, Tuple
from hgicommon.listenable import Listenable
from cookiemonster.common.collections import FileUpdateCollection
from cookiemonster.common.models import FileUpdate, FileProcessState
from cookiemonster.cookiejar._dbi import DBI
from cookiemonster.cookiejar._workflow import WorkflowDB, Event

# TODO Cookie, CookieCrumbs and CookieProcessState models

class CookieJar(Listenable):
    '''
    Manage and orchestrate data objects' metadata and the processing
    workflow
    '''
    def __init__(self, db_host: str, db_prefix: str):
        '''
        Constructor
        @param  db_host    Database host URL
        @param  db_prefix  Database name prefix
        TODO Others??
        '''
        pass

    def __call__(self, file_update: FileUpdate):
        '''
        Append/update metadata from a FileUpdate model. This is intended
        to be used as the listener to the upstream retriever and will
        convert the FileUpdate model's metadata into CookieCrumbs
        '''
        # TODO Is there any need to make a distinction between general
        # and specific metadata models, given they'll both ultimately be
        # of the same form?...

        # self.enrich_metadata(file_update.file_location, ...)
        pass

    def enrich_metadata(self, path: str, metadata: CookieCrumbs):
        '''
        Append/update metadata for a given file, thus changing its state
        and putting it back on the queue

        @param  path      File path
        @param  metadata  Metadata
        '''
        pass

    def mark_as_failed(self, path: str, requeue_delay: timedelta):
        '''
        Mark a file as having failed processing, thus returning it to
        the queue after a specified period

        @param  path           File path
        @param  requeue_delay  Time to wait before requeuing
        '''
        pass

    def mark_as_complete(self, path: str):
        '''
        Mark a file as having completed processing

        @param  path  File path
        '''
        pass

    def mark_as_reprocess(self, path: str):
        '''
        Mark a file for reprocessing, regardless of changes to its
        metadata, returning it to the queue immediately

        @param  path  File path
        '''
        pass

    def get_next_for_processing(self) -> CookieProcessState:
        '''
        Get the next Cookie for processing and update its state

        @return CookieProcessState
        '''
        pass

    def queue_length(self) -> int:
        '''
        Get the number of items ready for processing

        @return Number of items in the queue
        '''
        pass
