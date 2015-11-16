'''
Database Abstraction
====================

Operations for creating, checking and interacting with an SQLite-based
database.

Exportable classes: `DB`, `Event`

`DB` is to be instantiated with the SQLite database file (or in-memory
representation) and provides an interface with it. It will also build
the schema if it doesn't exist, or validate it if it does. Interface
methods are as follows:

* `add_new_model` Add a new model to the database and create the
  respective import log event

* `log_event_for_model` Log an arbitrary event against a model
  ("arbitrary" modulo applicable events; see below)

* `get_next_model_for_processing` Return the next FileUpdate model that
  requires processing (optionally marking it as such)

* `get_processing_queue_size` Return the number of FileUpdate models in
  the processing queue

`Event` is a simple enumeration class, not for instantiation, used with
the `DB.log_event` method. Enumerated constants are as follows:

* `imported`
* `processing`
* `completed`
* `failed`

Schema
------

    mgrFileUpdate      mgrLog         mgrEvents
    -------------      ---------      -----------
    id           --.   id         .-- id
    location       '-> file_id    |   description
    hash               event_id <-'   ttq
    timestamp          timestamp

All received FileUpdate models are put into mgrFileUpdate. Whenever an
event occurs against a FileUpdate, it is inserted in to mgrLog with a
timestamp (n.b., all timestamps are per Unix epoch). Events are:
imported, processing, completed (i.e., successfully) and failed; the IDs
of which are hardcoded as 1, 2, 3 and 4, respectively. (Note that an
insert trigger on mgrFileUpdate will automatically create the imported
event log entry.)

A view, mgrQueue, shows all the FileUpdate models that currently need to
be processed, in descending order of priority (i.e., the first record is
the head of the queue). Enqueuement is based upon each FileUpdate's
latest event's TTQ (time to queue, in seconds); priority is based on the
latest event's timestamp.

The schema is set up in such a way that only three operations are needed
against the database:

* Insert a new FileUpdate:
  insert into mgrFileUpdate(location, hash, timestamp) values ...

* Create a new log entry:
  insert into mgrLog(file_id, event_id) values ...

* Get the head of the queue:
  select location, hash, timestamp from mgrQueue limit 1

Default values and indices are appropriately defined.

Rant time! I totally get that the purpose of ORMs, such as SQLAlchemy,
is to optimise for maintenance (and, I suppose to some extent, obviate
any need on the developer to have a good knowledge of RDBMSs). That is,
we get an engine-agnostic abstraction that is relatively easy to play
with, without getting our hands dirty.

However, I don't have a problem with relational algebra -- and SQLite is
a delight -- and I defy any ORM to generate a schema as efficiently as I
can by hand. Sure, I have to write a bit of boilerplate, but at least I
know exactly how my persistence layer works!

Different strokes, I suppose... To any future maintainer (or my future
self, should I have a change of heart/lobotomy): Feel free to convert
this to whatever flavour-of-the-month abstraction you see fit.

Authors
-------
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015 Genome Research Limited
'''

import sqlite3
from time import mktime
from datetime import datetime
from cookiemonster.common.models import FileUpdate

# TODO Modicum of abstraction, rather than raw SQL calls... In general,
# this code is a bit of a big ball of mud and needs iterating against.

# TODO Testing code...

class _FileUpdateAdaptor(object):
    ''' Convert between FileUpdate model and DB representation '''
    @staticmethod
    def from_model(file_update, location_key='location', hash_key='hash', timestamp_key='timestamp'):
        '''
        Convert a FileUpdate model into a dictionary concordant with the
        database schema

        @param   file_update    FileUpdate model
        @param   location_key   Location key in return dictionary
        @param   hash_key       Hash key in return dictionary
        @param   timestamp_key  Timestamp key in return dictionary
        @return  Dictionary of parameters with aforementioned keys
        '''
        return {
            location_key:  file_update.file_location,
            hash_key:      file_update.file_hash,
            timestamp_key: int(mktime(file_update.timestamp.timetuple()))
        }

    @staticmethod
    def to_model(row, location_index=0, hash_index=1, timestamp_index=2):
        '''
        Convert a database row into a FileUpdate model

        @param   row              Database row
        @param   location_index   Location index in row tuple
        @param   hash_index       Hash index in row tuple
        @param   timestamp_index  Timestamp index in row tuple
        @return  FileUpdate model
        '''
        return FileUpdate(
            row[location_index],
            row[hash_index],
            datetime.fromtimestamp(row[timestamp_index])
        )

class Event(object):
    ''' Event enumeration '''
    imported   = 1
    processing = 2
    completed  = 3
    failed     = 4

class DB(object):
    '''
    Create (if necessary) and check the database schema, plus
    provide the interface to interact with the data.
    '''

    def __init__(self, database):
        '''
        Connect to specified database and validate the schema

        @param  database  SQLite database file
        '''
        self._conn = sqlite3.connect(database, isolation_level=None)

        if self._conn:
            self._validate_schema()

        else:
            raise sqlite3.InterfaceError('Could not connect to %s' % database)

    def __del__(self):
        ''' Close database connection '''
        if self._conn:
            self._conn.close()

    def _get_file_id_by_model(self, file_update):
        ''' Get ID of FileUpdate '''
        cur = self._conn.execute('''
            select id
            from   mgrFileUpdate
            where  location  = :location
            and    hash      = :hash
            and    timestamp = :timestamp
        ''', _FileUpdateAdaptor.from_model(file_update))
        row = cur.fetchone()

        if row:
            return row[0]

        else:
            raise sqlite3.DataError('No such FileUpdate')

    def add_new_model(self, new_file):
        '''
        Add a new FileUpdate to the database

        @param   new_file  FileUpdate model
        @return  Success (Boolean)
        '''
        try:
            self._conn.execute('''
                insert into mgrFileUpdate(location,  hash,  timestamp)
                            values       (:location, :hash, :timestamp)
            ''', _FileUpdateAdaptor.from_model(new_file))
            return True

        except:
            return False

    def log_event_for_model(self, file_update, event_id):
        '''
        Add a new event for a FileUpdate

        @param   file_update  FileUpdate model
        @param   event_id     Event ID
        @return  Success (Boolean)
        '''
        try:
            file_id = self._get_file_id_by_model(file_update)
            self._conn.execute('''
                insert into mgrLog(file_id,  event_id)
                            values(:file_id, :event_id)
            ''', {'file_id': file_id, 'event_id': event_id})
            return True

        except:
            return False

    def get_next_model_for_processing(self, auto_log_for_processing=True):
        '''
        Get the next FileUpdate for processing and update its state

        @param   auto_log_for_processing  Automatically create the
                                          processing event for the
                                          retrieved model
        @return  FileUpdate model, or None
        '''
        cur = self._conn.execute('''
            select location,
                   hash,
                   timestamp
            from   mgrQueue
            limit  1
        ''')
        row = cur.fetchone()

        if row:
            to_process = _FileUpdateAdaptor.to_model(row)
            if auto_log_for_processing:
                self.log_event_for_model(to_process, Event.processing)

            return to_process

        else:
            return None

    def get_processing_queue_size(self):
        '''
        Get the number of items ready for processing

        @return Number of items in the queue
        '''
        cur = self._conn.execute('''
            select queue_size
            from   mgrQueueSize
        ''')
        row = cur.fetchone()

        if row:
            return row[0]

        else:
            return 0

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
        self._conn.executescript('''
            create table if not exists mgrFileUpdate (
                id         integer  primary key,
                location   text     not null,
                hash       text     not null,
                timestamp  integer  not null,

                unique (location, hash, timestamp)
            );

            create table if not exists mgrEvents (
                id           integer  primary key on conflict ignore,
                description  text     unique on conflict ignore
                                      not null,
                ttq          integer  default (null)
                                      check (ttq is null or ttq >= 0)
            );

            insert into mgrEvents(id, description,  ttq)
                        values   (1,  'imported',   0),
                                 (2,  'processing', null),
                                 (3,  'completed',  null),
                                 (4,  'failed',     5 * 24 * 60 * 60);

            create table if not exists mgrLog (
                id         integer  primary key,
                file_id    integer  references mgrFileUpdate(id)
                                    not null,
                event_id   integer  references mgrEvents(id)
                                    not null,
                timestamp  integer  not null
                                    default (strftime('%s', 'now'))
            );

            create index if not exists mgrIdxLog on mgrLog (file_id, timestamp asc);

            create index if not exists mgrIdxLogTime on mgrLog (timestamp asc);

            create trigger if not exists mgrNewFileUpdate
                after insert on mgrFileUpdate for each row
                begin
                    insert into mgrLog(file_id, event_id) values (new.id, 1);
                end;

            create view if not exists mgrQueue as
                select    mgrFileUpdate.location,
                          mgrFileUpdate.hash,
                          mgrFileUpdate.timestamp
                from      mgrLog latest
                left join mgrLog later
                on        later.file_id    = latest.file_id
                and       later.timestamp  > latest.timestamp
                join      mgrFileUpdate
                on        mgrFileUpdate.id = latest.file_id
                join      mgrEvents
                on        mgrEvents.id     = latest.event_id
                and       mgrEvents.ttq   is not null
                where     later.id is null
                and       cast(strftime('%s', 'now') as integer) > latest.timestamp + mgrEvents.ttq
                order by  latest.timestamp asc;
            
            create view if not exists mgrQueueSize as
                select count(*) queue_size
                from   mgrQueue;

            vacuum;
        ''')
