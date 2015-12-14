"""
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
"""
from datetime import datetime
from enum import Enum
from enum import unique
from functools import total_ordering
from typing import Any, Union, Set, Optional, TypeVar, Generic
from hgicommon.collections import Metadata
from hgicommon.models import Model

import cookiemonster
from cookiemonster.common.enums import EnrichmentSource


class Update(Model):
    """
    Model of a file update.
    """
    def __init__(self, target: str, hash: hash, timestamp: datetime, metadata: Metadata):
        self.target = target
        self.hash = hash
        self.timestamp = timestamp
        self.metadata = metadata


@total_ordering
class Enrichment(Model):
    """
    Metadata enrichment model
    """
    def __init__(self, source: Union[EnrichmentSource, str], timestamp: datetime, metadata: Metadata):
        self.source    = source
        self.timestamp = timestamp
        self.metadata  = metadata

    def __lt__(self, other):
        """ Order enrichments by their timestamp """
        return (self.timestamp < other.timestamp)


class Cookie(Model):
    """
    A "Cookie" is a representation of a file's iteratively enriched metadata.
    """
    def __init__(self, path: str):
        self.path = path
        self.enrichments = []

    def enrich(self, enrichment: Enrichment):
        """
        Append an enrichment

        @param  enrichment  The enrichment
        """
        self.enrichments.append(enrichment)

    def get_metadata_by_source(self, source: Union[EnrichmentSource, str], key: str, default=None):
        """
        Fetch the latest existing metadata by source and key

        @param  source   Enrichment source
        @param  key      Attribute name
        @param  default  Default value, if key doesn't exist
        """
        # The enrichment collection will be built up chronologically, so
        # the following list comprehension is guaranteed to be in the
        # same relative order, thus we can check from the last to the
        # first for a match, to get the most recent
        return next((
            enrichment.metadata[key]
            for enrichment in reversed(self.enrichments)
            if  enrichment.source == source
            and key in enrichment.metadata
        ), default)

    def get_metadata_sources(self) -> Set[Union[EnrichmentSource, str]]:
        """
        Fetch the distinct enrichment sources for which metadata exists
        """
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
