'''
Data Manager
============

Listen for file updates from the retriever, store them and manage their
processing workflow by interfacing with the rule engine processor. This
implies that the data manager becomes the authoritative source for the
processing workflow.

Exportable classes: `DataManager`

`DataManager` is to be instantiated with an address to its SQLite
database and the lead time for failed jobs to reappear on the queue.
Otherwise, it listens to the retriever for updates and provides a
Listenable interface to the rule engine processor: the effect being that
of a message rippling through from the retriever, through the data
manager and on to the rule engine processor.  Moreover, the rule engine
processor requires some methods to communicate with the data manager:

* `file_update_retrieval_listener` The is the listener interface to the
  retriever. When the retriever sends a message to it, the import
  process is started and, ultimately, will broadcast a message to the
  rule engine processor. Note that the message will be the current queue
  size and will be broadcast regardless of any changes; as such, it will
  have the same period as the retriever (this is so failed jobs won't
  get forgotten)

* `get_next_model_for_processing` Return the next FileUpdate model that
  requires processing

* `get_processing_queue_size` Return the number of FileUpdate models in
  the [pending for] processing queue; this is also what is broadcast to
  the rule engine processor (and any other listeners)

* `mark_model_as_completed` Mark a FileUpdate model as having been
  successfully processed

* `mark_model_as_failed` Mark a FileUpdate model as having failed its
  processing; this will put it back on the queue, after the specified
  lead time

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015 Genome Research Limited
'''

from datetime import timedelta
from cookiemonster.common.listenable import Listenable
from cookiemonster.common.models import FileUpdate
from cookiemonster.common.collections import FileUpdateCollection
from cookiemonster.manager._db import DB, Event

# TODO Testing code...

# TODO Type hints

class DataManager(Listenable):
    '''
    Manage the FileUpdate processing workflow
    '''
    def __init__(self, database, failure_lead_time):
        '''
        @param  database  SQLite database
        '''
        super(DataManager, self).__init__()
        self._db = DB(database, failure_lead_time)
        self._listeners = []

    def file_update_retrieval_listener(self, file_updates):
        '''
        Listen to the retriever and import all the new FileUpdates that
        it broadcasts.

        @param  file_updates  Collection of file updates
        '''
        for file_update in file_updates:
            self._db.add_new_model(file_update)
        
        # Broadcast queue size
        queue_size = self.get_processing_queue_size()
        if queue_size > 0:
            self.notify_listeners(queue_size)

    def get_next_model_for_processing(self):
        '''
        Get the next FileUpdate for processing and update its sate

        @return FileUpdate model, or None
        '''
        return self._db.get_next_model_for_processing()

    def get_processing_queue_size(self):
        '''
        Get the number of items ready for processing

        @return Number of items in the queue
        '''
        return self._db.get_processing_queue_size()

    def mark_model_as_completed(self, file_update):
        '''
        Mark a model as completed successfully

        @param  file_update  FileUpdate model
        @return Success of updating workflow (Boolean)
        '''
        return self._db.log_event_for_model(file_update, Event.completed)

    def mark_model_as_failed(self, file_update):
        '''
        Mark a model as failed processing

        @param  file_update  FileUpdate model
        @return Success of updating workflow (Boolean)
        '''
        return self._db.log_event_for_model(file_update, Event.failed)
