'''
Workflow Database Abstraction
=============================

Operations for managing the SQLite-based workflow database; including
its creation, checking and interaction.

Exportable classes: `WorkflowDB`, `Event`

Event
-----
A simple enumeration class (i.e., not for instantiation) used to model
the workflow. Constants are as follows:

* `imported`
* `processing`
* `completed`
* `failed`
* `reprocess`

WorkflowDB
----------
`WorkflowDB` is to be instantiated with the SQLite database file (or in-
-memory representation), a metadata database object and the failure lead
time. As far as the SQLite schema is concerned: the class will build it
if it doesn't exist, or validate it otherwise.

Methods:

* `upsert` Insert/update a FileUpdate model into the database

* `log` Log an arbitrary event against a model ("arbitrary" modulo
  applicable events per `Event`)

* `next` Get the next FileUpdate model that requires processing

* `length` Return the length of the queue

Schema
------

        mgrFiles        mgrFileMeta       mgrFileStates
        ---------       -----------       -------------
    .-- id        --.   id            .-- id
    |   path        '-> file_id       |   description
    |                   state_id    <-'
    |                   hash
    |                   timestamp
    |                   metadata_id ····> (Metadata DB)
    |
    |
    |   mgrLog          mgrEvents
    |   ---------       ---------
    |   id          .-- id
    '-> file_id     |   description
        event_id  <-'   ttq
        timestamp

Files are the atomic unit, but their metadata (timestamp, hash and iRODS
metadata) can change. A FileUpdate model contains all the above
information, but is stored in the database per the above. Note that
iRODS metadata is stored in an external database by `mgrFiles.id` and a
revision key (`mgrFileMeta.metadata_id`).

The workflow for each file is kept in a log (`mgrLog`) and the queue for
upstream processing is derived from this (as the view `mgrQueue`) using
each file's latest record's TTQ ("time to queue", in seconds), where
priority is based on the latest event's timestamp.

It is possible that an update to a file will come while that file is
currently being processed upstream. As such, we keep up to two versions
of each file's metadata: the latest and the "inflight", for when a file
is being processed. A discrepancy between these two versions indicate
that a file will need reprocessing.

In the event of an upstream crash, orphaned "processing" (and, for
inflight changes, "reprocess") records should be removed at startup,
which will put them back on the queue for processing. If this step were
omitted, said files would be forever, erroneously marked as inflight,
but never actually get processed.

Notes:
* All timestamps are per the Unix epoch.
* The records in `mgrEvents` ought to match the `Events` enumeration,
  with the additional TTQ value (or null, if said event is not relevant
  for enqueueing).
* If multiple reprocessing requests are issued for any file, this is
  only stored once on the log (updating the timestamp), rather than
  creating multiple reprocess records.
* If the upstream processor finishes with a file, but there has been an
  interim reprocessing request, the processing status (success/fail) is
  not recorded.
* A reprocessing request must not be fulfilled while a file is currently
  in flight.

FIXME? This schema is not ideal, as attested by the above policy on
logic that has to be handled externally.

FIXME I'm not using transactions productively, so there's a chance that
data will go out of sync in the event of a crash or concurrent updates

Dependencies
------------
* SQLite 3.7.11, or later

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015 Genome Research Limited
'''

import sqlite3
from typing import Optional, Tuple
from enum import Enum
from time import mktime
from datetime import datetime, timedelta
from cookiemonster.common.models import Metadata, FileUpdate
from cookiemonster.manager._metadata import MetadataDB

# TODO Modicum of abstraction, rather than raw SQL calls... In general,
# this code is a bit of a big ball of mud and needs iterating against.

# TODO Testing code...

# TODO Determine minimal compatible version of SQLite
_sqlite3_version_required = (3, 7, 11)
if sqlite3.sqlite_version_info < _sqlite3_version_required:
    error_text = 'Requires SQLite {required}, or newer. You have {actual}.'.format(
        required='.'.join(str(i) for i in _sqlite3_version_required),
        actual=sqlite3.sqlite_version
    )
    raise sqlite3.NotSupportedError(error_text)

def _unix_time(timestamp: datetime) -> int:
    ''' Convert datetime into Unix epoch '''
    return int(mktime(timestamp.timetuple()))

class Event(Enum):
    ''' Event enumeration '''
    imported   = 1
    processing = 2
    completed  = 3
    failed     = 4
    reprocess  = 5

class _Status(Enum):
    ''' File metadata status enumeration '''
    latest   = 1
    inflight = 2

class WorkflowDB(object):
    '''
    Create (if necessary) and check the database schema, plus provide
    the interface to interact with the data.
    '''
    def __init__(self, workflow_db: str, metadata_db: MetadataDB, failure_lead_time: timedelta):
        '''
        Connect to specified database and validate the schema

        @param  workflow_db        SQLite database address
        @param  metadata_db        Metadata database object
        @param  failure_lead_time  Time before failures are requeued
        '''
        self._db = {
            'workflow': sqlite3.connect(workflow_db, isolation_level=None),
            'metadata': metadata_db
        }

        sql = self._db['workflow']
        if not sql:
            raise sqlite3.InterfaceError('Could not connect to %s' % workflow_db)

        self._validate_schema()

        # Set the failure lead time
        sql.execute('''
            update mgrEvents
            set    ttq = :ttq
            where  id  = :failed_id
        ''', {
            'failed_id': Event.failed.value,
            'ttq':       int(failure_lead_time.total_seconds())
        })

    def __del__(self):
        ''' Close database connection '''
        sql = self._db['workflow']
        if sql:
            sql.close()

    def _get_id(self, file_update: FileUpdate) -> Optional[int]:
        '''
        Get the file ID of a FileUpdate

        @param  file_update  FileUpdate model
        @return The file ID (None, if not found)
        '''
        sql = self._db['workflow']
        file_id, = sql.execute('''
            select id
            from   mgrFiles
            where  path = :file_path
        ''', {
            'file_path': file_update.file_location
        }).fetchone() or (None,)

        return file_id

    def _get_event(self, file_id: int) -> Tuple[int, Event]:
        '''
        Get the latest event for a file by its ID

        @param  file_id  File ID
        @return The latest Event, with its ID
        '''
        sql = self._db['workflow']
        log_id, event_id = sql.execute('''
            select id,
                   event_id
            from   mgrFileStatus
            where  file_id = :file_id
        ''', {
            'file_id': file_id
        }).fetchone()

        return log_id, Event(event_id)

    def _fetch(self, file_id: int, status: _Status = _Status.latest) -> Optional[FileUpdate]:
        '''
        Fetch the FileUpdate model from the database by its file ID and
        optional status

        @param   file_id  File ID
        @param   status   File metadata status (default _Status.latest)
        @return  FileUpdate model (None, if not found)
        '''
        sql = self._db['workflow']
        mdb = self._db['metadata']

        # Get file path
        file_location, = sql.execute('''
            select path
            from   mgrFiles
            where  id = :file_id
        ''', {
            'file_id': file_id
        }).fetchone() or (None,)

        if not file_location:
            return None

        # Get metadata
        file_hash, file_timestamp, metadata_id = sql.execute('''
            select hash,
                   timestamp,
                   metadata_id
            from   mgrFileMeta
            where  file_id  = :file_id
            and    state_id = :state_id
        ''', {
            'file_id':  file_id,
            'state_id': status.value
        }).fetchone() or (None,) * 3

        if not metadata_id:
            return None

        # Get iRODS metadata
        metadata = mdb.fetch(file_id, metadata_id)
        if not metadata:
            return None

        return FileUpdate(
            file_location,
            file_hash,
            datetime.fromtimestamp(file_timestamp),
            metadata
        )

    def _log_by_id(self, file_id: int, event_id: Event):
        '''
        Log an arbitrary event against a file ID, while keeping the log
        and metadata state consistent. Specifically:
        
        * Each file should have at most one "reprocess" event; a new
          "reprocess" event should just update the current ones
          timestamp
        * A "completed" or "failed" event should delete the inflight
          metadata record for that file
        * A "completed" or "failed" event should not be logged if there
          is a pending "reprocess"

        @param  file_id   File ID
        @param  event_id  Event
        '''
        sql = self._db['workflow']
        write_log = True

        if event_id is Event.reprocess:
            # Update any existing reprocess event, rather
            # than create a new one
            log_id, current_event = self._get_event(file_id)
            if current_event is Event.reprocess:
                write_log = False
                sql.execute('''
                    update mgrLog
                    set    timestamp = strftime('%s', 'now')
                    where  id        = :log_id
                ''', {
                    'log_id': log_id   
                })

        elif event_id in [Event.completed, Event.failed]:
            # Don't log if we want to reprocess
            _, current_event = self._get_event(file_id)
            if current_event is Event.reprocess:
                write_log = False

            # Delete inflight record
            sql.execute('''
                delete
                from   mgrFileMeta
                where  file_id  = :file_id
                and    state_id = :state_id
            ''', {
                'file_id':  file_id,
                'state_id': _Status.inflight.value
            })

        if write_log:
            sql.execute('''
                insert into mgrLog(file_id,  event_id)
                            values(:file_id, :event_id)
            ''', {
                'file_id':  file_id,
                'event_id': event_id.value
            })

    def log(self, file_update: FileUpdate, event_id: Event):
        '''
        Log an arbitrary event against a FileUpdate model
        
        @param  file_update FileUpdate model
        @param  event_id    Event

        n.b., This is just a wrapper over the internal `_log_by_id`
        '''
        file_id = self._get_id(file_update)
        if file_id:
            self._log_by_id(file_id, event_id)

    def upsert(self, file_update: FileUpdate) -> int:
        '''
        Insert or update a FileUpdate model and update the event log
        appropriately

        @param  file_update FileUpdate model
        @return File ID

        n.b., If nothing has changed, on update, then the event log will
        not need updating
        '''
        sql = self._db['workflow']
        mdb = self._db['metadata']

        file_id      = self._get_id(file_update)
        new_file     = False
        new_metadata = False

        if file_id:
            # Get metadata
            meta_id, file_hash, file_timestamp, metadata_id = sql.execute('''
                select id,
                       hash,
                       timestamp,
                       metadata_id
                from   mgrFileMeta
                where  file_id  = :file_id
                and    state_id = 1
            ''', {
                'file_id':  file_id
            }).fetchone()

            # Get iRODS metadata
            metadata = mdb.fetch(file_id, metadata_id)

            new_metadata = (file_hash      != file_update.file_hash) \
                        or (file_timestamp != _unix_time(file_update.timestamp)) \
                        or (metadata       != file_update.metadata)

        else:
            # Create new record
            cursor = sql.cursor()
            cursor.execute('''
                insert into mgrFiles(path)
                            values  (:file_path)
            ''', {
                'file_path': file_update.file_location   
            })
            
            file_id      = cursor.lastrowid
            new_file     = True
            new_metadata = True
            
        if new_metadata:
            if new_file:
                # Insert new metadata
                sql.execute('''
                    insert into mgrFileMeta(file_id,  state_id,  hash,  timestamp,  metadata_id)
                                values     (:file_id, :state_id, :hash, :timestamp, :metadata_id)
                ''', {
                    'file_id':     file_id,
                    'state_id':    _Status.latest.value,
                    'hash':        file_update.file_hash,
                    'timestamp':   _unix_time(file_update.timestamp),
                    'metadata_id': mdb.upsert(file_id, file_update.metadata)
                })

            else:
                # Update existing metadata
                sql.execute('''
                    update mgrFileMeta
                    set    hash        = :hash,
                           timestamp   = :timestamp,
                           metadata_id = :metadata_id
                    where  id          = :meta_id
                ''', {
                    'meta_id':     meta_id,
                    'hash':        file_update.file_hash,
                    'timestamp':   _unix_time(file_update.timestamp),
                    'metadata_id': mdb.upsert(file_id, file_update.metadata)
                })

        # Write the log entry
        self._log_by_id(file_id, Event.imported if new_file else Event.reprocess)

        return file_id

    def next(self) -> Optional[FileUpdate]:
        '''
        Dequeue the next FileUpdate to be processed and update the event
        log appropriately

        @return Next FileUpdate to process (None, if empty)
        '''
        sql = self._db['workflow']
        next_id, = sql.execute('''
            select file_id
            from   mgrQueue
            limit  1
        ''').fetchone() or (None,)

        if not next_id:
            return None

        # Mark as "processing" and create inflight record
        self._log_by_id(next_id, Event.processing)
        sql.execute('''
            insert into mgrFileMeta(file_id, state_id, hash, timestamp, metadata_id)
                select file_id,
                       2,
                       hash,
                       timestamp,
                       metadata_id
                from   mgrFileMeta
                where  file_id  = :file_id
                and    state_id = :state_id
        ''', {
            'file_id':  next_id,
            'state_id': _Status.latest.value
        })

        return self._fetch(next_id)

    def length(self) -> int:
        '''
        @return The current queue length
        '''
        sql = self._db['workflow']
        return sql.execute('''
            select queue_size
            from   mgrQueueSize
        ''').fetchone()[0]

    def _validate_schema(self):
        '''
        Build/check the database schema, where necessary, by SQL script
        I admit this is pretty ugly...but deal with it :P

        TODO See above regarding abstraction
        TODO Schema checking (beyond existence): One could insert valid
             and invalid data into the schema (and rolling back the
             transaction) and check the exceptions. It's not pretty, but
             it's much easier/less verbose than poking around the data
             dictionary...
        '''
        sql = self._db['workflow']
        sql.executescript('''
            /* Tables */
            create table if not exists mgrFiles (
                id           integer  primary key,
                path         text     unique on conflict rollback
                                      not null
            );

            create table if not exists mgrFileStates (
                id           integer  primary key,
                description  text     unique
                                      not null
            );

            create table if not exists mgrFileMeta (
                id           integer  primary key,
                file_id      integer  references mgrFiles(id)
                                      not null,
                state_id     integer  references mgrFileStates(id)
                                      not null,
                hash         text     not null,
                timestamp    integer  not null,
                metadata_id  text     not null
            );

            create table if not exists mgrEvents (
                id           integer  primary key,
                description  text     unique
                                      not null,
                ttq          integer  default (null)
                                      check (ttq is null or ttq >= 0)
            );

            create table if not exists mgrLog (
                id           integer  primary key,
                file_id      integer  references mgrFileUpdate(id)
                                      not null,
                event_id     integer  references mgrEvents(id)
                                      not null,
                timestamp    integer  not null
                                      default (strftime('%s', 'now'))
            );

            /* Indices */
            create index if not exists mgrIdxLog on mgrLog (file_id, id asc);
            create index if not exists mgrIdxLogTime on mgrLog (timestamp asc);

            /* Enumerations */
            insert or replace into mgrFileStates (id, description)
                                   values        (1,  'latest'),
                                                 (2,  'inflight');

            insert or replace into mgrEvents     (id, description,  ttq)
                                   values        (1,  'imported',   0),
                                                 (2,  'processing', null),
                                                 (3,  'completed',  null),
                                                 (4,  'failed',     null),
                                                 (5,  'reprocess',  0);
            /* Views */
            create view if not exists mgrFileStatus as
                select    latest.id,
                          latest.file_id,
                          latest.event_id,
                          latest.timestamp
                from      mgrLog latest
                left join mgrLog later
                on        later.file_id = latest.file_id
                and       later.id      > latest.id
                where     later.id is null;

            create view if not exists mgrQueue as
                select    mgrFileStatus.file_id
                from      mgrFileStatus
                join      mgrEvents
                on        mgrEvents.id         = mgrFileStatus.event_id
                and       mgrEvents.ttq       is not null
                left join mgrFileMeta
                on        mgrFileMeta.file_id  = mgrFileStatus.file_id
                and       mgrFileMeta.state_id = 2 /* inflight */
                where     mgrFileMeta.id is null
                and       cast(strftime('%s', 'now') as integer) >= mgrFileStatus.timestamp + mgrEvents.ttq
                order by  mgrFileStatus.timestamp asc;

            create view if not exists mgrQueueSize as
                select count(*) queue_size
                from   mgrQueue;

            /* Crash Clean-Up
               Delete all orphaned "reprocess" logs, then all
               "processing" logs. This MUST be done in this order. */

            delete from mgrLog where id in (
                select    latest.id
                from      mgrLog latest
                left join mgrLog later
                on        later.file_id    = latest.file_id
                and       later.id         > latest.id
                where     later.id        is null
                and       latest.event_id  = 5 /* reprocess */
            );

            delete from mgrLog where id in (
                select    latest.id
                from      mgrLog latest
                left join mgrLog later
                on        later.file_id    = latest.file_id
                and       later.id         > latest.id
                where     later.id        is null
                and       latest.event_id  = 2 /* processing */
            );

            delete from mgrFileMeta where state_id = 2; /* inflight */

            vacuum;
        ''')
