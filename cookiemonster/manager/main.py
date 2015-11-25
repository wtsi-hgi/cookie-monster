'''
Data Manager
============
Listen for file updates from the retriever, store them and manage their
processing workflow by interfacing with the rule engine processor. This
implies that the data manager becomes the authoritative source for the
processing workflow.

Exportable classes: `DataManager`

DataManager
-----------
`DataManager` is to be instantiated with an address to its SQLite
database, the host URL and name of its CouchDB database and the lead
time for failed jobs to reappear on the queue. Otherwise, it listens to
the retriever for updates and provides a Listenable interface for any
downstream processing: the effect being that of a message rippling
through from the retriever, through the data manager and on to
downstream processing. Said processing will need to send messages back
to the data manager via the following methods:

* `get_next_for_processing` Return the next FileProcessState model that
  requires processing (i.e., the current FileUpdate to process and its
  last processed state)

* `queue_length` Return the number of FileUpdate models in the [pending
  for] processing queue

* `mark_as_successful` Mark a FileUpdate model as having been
  successfully processed

* `mark_as_failed` Mark a FileUpdate model as having failed its
  processing; this will return it to the queue, after the specified lead
  time

* `mark_as_reprocess` Mark a FileUpdate model as requiring reprocessing;
  this will return it to the queue immediately, once any currently
  inflight processing has completed

An instantiated `DataManager` is callable and this acts as the listener
to the retriever. When a message is sent to it, the import process is
started and, ultimately, the data manager will broadcast its own message
for downstream listeners. That message will be the current queue length,
regardless of any changes.

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015 Genome Research Limited
'''

# TODO Testing code...

from datetime import timedelta
from typing import Optional, Union, Tuple
from hgicommon.listenable import Listenable
from cookiemonster.common.collections import FileUpdateCollection
from cookiemonster.common.models import FileUpdate, FileProcessState
from cookiemonster.manager._metadata import MetadataDB
from cookiemonster.manager._workflow import WorkflowDB, Event

class DataManager(Listenable):
    '''
    Manage and orchestrate the FileUpdate processing workflow
    '''
    def __init__(self, workflow_db: str, metadata_host: str, metadata_db: str, failure_lead_time: timedelta):
        '''
        Constructor
        
        @param  workflow_db        Workflow database address
        @param  metadata_host      Metadata database host
        @param  metadata_db        Metadata database name
        @param  failure_lead_time  Time before failures are requeued
        '''
        super().__init__()
        self._mdb = MetadataDB(metadata_db, metadata_host)
        self._workflow = WorkflowDB(workflow_db, self._mdb, failure_lead_time)
        self._listeners = []

    def __call__(self, file_updates: FileUpdateCollection):
        '''
        Listen to the retriever and import all the new FileUpdates that
        it broadcasts

        @param  file_updates  Collection of file updates
        '''
        for file_update in file_updates:
            self._workflow.upsert(file_update)
        
        # Broadcast queue size
        queue_size = self.queue_length()
        if queue_size > 0:
            self.notify_listeners(queue_size)

    def get_next_for_processing(self) -> Optional[FileProcessState]:
        '''
        Get the next FileUpdate for processing and update its state

        @return FileProcessState (None, if nothing found)
        '''
        return self._workflow.dequeue()

    def queue_length(self) -> int:
        '''
        Get the number of items ready for processing

        @return Number of items in the queue
        '''
        return self._workflow.length()

    def mark_as_completed(self, file_update: FileUpdate):
        '''
        Mark a model as completed successfully

        @param  file_update  FileUpdate model
        '''
        return self._workflow.log(file_update, Event.completed)

    def mark_as_failed(self, file_update: FileUpdate):
        '''
        Mark a model as failed processing

        @param  file_update  FileUpdate model
        '''
        return self._workflow.log(file_update, Event.failed)

    def mark_as_reprocess(self, file_update: FileUpdate):
        '''
        Mark a model for reprocessing

        @param  file_update  FileUpdate model
        '''
        return self._workflow.log(file_update, Event.reprocess)
