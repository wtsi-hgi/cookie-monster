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
from datetime import datetime
from functools import total_ordering
from typing import Any, Union, Set, Optional

from hgicommon.collections import Metadata
from hgicommon.models import Model

import cookiemonster
from cookiemonster.common.enums import EnrichmentSource


class FileUpdate(Model):
    """
    Model of a file update.
    """
    def __init__(self, file_id: str, file_hash: hash, timestamp: datetime, metadata: Metadata):
        """
        Constructor.
        :param file_id: a unique identifier of the file that has been updated
        :param file_hash: hash of the file
        :param timestamp: the timestamp of when the file was updated
        :param metadata: the metadata of the file
        """
        self.file_id = file_id
        self.file_hash = file_hash
        self.timestamp = timestamp
        self.metadata = metadata


@total_ordering
class Enrichment(Model):
    '''
    Metadata enrichment model
    '''
    def __init__(self, source: Union[EnrichmentSource, str], timestamp: datetime, metadata: Metadata):
        '''
        Constructor

        @param  source     Source of metadata enrichment
        @param  timestamp  Timestamp of enrichment
        '''
        self.source    = source
        self.timestamp = timestamp
        self.metadata  = metadata

    def __lt__(self, other):
        ''' Order enrichments by their timestamp '''
        return (self.timestamp < other.timestamp)


class Cookie(Model):
    '''
    A "Cookie" is a representation of a file's iteratively enriched
    metadata
    '''
    def __init__(self, path: str):
        '''
        Constructor
        @param  path  File path
        '''
        self.path = path
        self.enrichments = cookiemonster.common.collections.EnrichmentCollection()

    def enrich(self, enrichment: Enrichment):
        '''
        Append an enrichment

        @param  enrichment  The enrichment
        '''
        self.enrichments.append(enrichment)

    def get_metadata_by_source(self, source: Union[EnrichmentSource, str], key: str, default=None):
        '''
        Fetch the latest existing metadata by source and key

        @param  source   Enrichment source
        @param  key      Attribute name
        @param  default  Default value, if key doesn't exist
        '''
        # The enrichment collection will be built up chronologically, so
        # the following list comprehension is guaranteed to be in the
        # same relative order...
        sourced = [enrichment for enrichment in self.enrichments if enrichment.source == source]

        # ...thus we can check from the last to the first for a match,
        # to get the most recent
        return next((enrichment.metadata[key] for enrichment in reversed(sourced) if key in enrichment.metadata), default)

    def get_metadata_sources(self) -> Set[Union[EnrichmentSource, str]]:
        '''
        Fetch the distinct enrichment sources for which metadata exists
        '''
        return {enrichment.source for enrichment in self.enrichments}


class Notification(Model):
    """
    A model of a notification that should be sent to an external process.
    """
    def __init__(self, external_process_name: str, data: Optional[Any]=None):
        """
        Default constructor.
        :param external_process_name: the name of the external process that should be informed
        :param data: the data (if any) that should be given to the external process
        """
        self.external_process_name = external_process_name
        self.data = data
