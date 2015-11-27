'''
Common Models
=============

Authors
-------
* Colin Nolan <cn13@sanger.ac.uk>
* Christopher Harrison <ch12@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015 Genome Research Limited
'''
from datetime import date
from typing import Any, Optional

from hgicommon.models import Model
from hgicommon.collections import Metadata


class IRODSMetadata(Metadata):
    '''
    IRODS metadata is in the form of "AVUs" (attribute-value-unit
    tuples). We disregard the unit because we aren't using them.
    Otherwise, attributes may have many values. For comparisons sake,
    we therefore canonicalise all attributes for a value into an ordered
    list of distinct elements.
    '''
    def set(self, key, value):
        '''
        Canonicalise the value before insertion
        '''
        canonical_value = sorted(set(value)) if type(value) is list else [value]
        super().set(key, canonical_value)


class FileUpdate(Model):
    """
    Model of a file update.
    """
    def __init__(self, file_location: str, file_hash: hash, timestamp: date, metadata: IRODSMetadata):
        """
        Constructor.
        :param file_location: the location of the file that has been updated
        :param file_hash: hash of the file
        :param timestamp: the timestamp of when the file was updated
        :param metadata: the metadata of the file
        """
        self.file_location = file_location
        self.file_hash = file_hash
        self.timestamp = timestamp
        self.metadata = metadata


class CookieCrumbs(Metadata):
    '''
    CookieCrumbs is just an alias of the generic metadata model
    '''
    pass


class Cookie(Model):
    '''
    A "Cookie" is a representation of a file's "complete" metadata, in
    as much as its ever complete
    '''
    def __init__(self, path: str):
        '''
        Constructor
        @param  path  File path
        '''
        self.path     = path
        self.metadata = CookieCrumbs()

    # TODO? Enrich method...


class CookieProcessState(Model):
    '''
    Model file processing state
    '''
    def __init__(self, current_state: Cookie, processed_state: Optional[Cookie]=None):
        '''
        Constructor

        @param  current_state   Current Cookie for processing
        @param  processed_state Previously processed Cookie
        '''
        self.path            = current_state.path
        self.current_state   = current_state.metadata
        self.processed_state = processed_state.metadata if processed_state else None

        # TODO? Generate diff automagically...


class Notification(Model):
    """
    A model of a notification that should be sent to an external process.
    """
    def __init__(self, external_process_name: str, data: Any=None):
        """
        Default constructor.
        :param external_process_name: the name of the external process that should be informed
        :param data: the data (if any) that should be given to the external process
        """
        self.external_process_name = external_process_name
        self.data = data
