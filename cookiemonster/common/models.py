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
from functools import total_ordering
from typing import Any, Union, Set, Optional
from typing import Iterable

from hgicommon.collections import Metadata
from hgicommon.models import Model

from cookiemonster.common.enums import EnrichmentSource


class Update(Model):
    """
    Model of a file update.
    """
    def __init__(self, target: str, timestamp: datetime, metadata: Metadata = Metadata()):
        self.target = target
        self.timestamp = timestamp
        self.metadata = metadata


@total_ordering
class Enrichment(Model):
    """
    Metadata enrichment model
    """
    def __init__(self, source: Union[EnrichmentSource, str], timestamp: datetime, metadata: Metadata):
        self.source = source
        self.timestamp = timestamp
        self.metadata = metadata

    def __lt__(self, other):
        """ Order enrichments by their timestamp """
        return self.timestamp < other.timestamp


class Cookie(Model):
    """
    A "Cookie" is a representation of a file's iteratively enriched metadata.
    """
    def __init__(self, path: str):
        self.path = path
        self.enrichments = []   # type: Iterable(Enrichment)

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
            if enrichment.source == source
               and key in enrichment.metadata
        ), default)

    def get_metadata_sources(self) -> Set[Union[EnrichmentSource, str]]:
        """
        Fetch the distinct enrichment sources for which metadata exists
        """
        return {enrichment.source for enrichment in self.enrichments}


class Notification(Model):
    """
    A model of a notification that should be sent to a receiver.
    """
    def __init__(self, about: str, data: Any=None, sender: str=None):
        """
        Constructor.
        :param about: what the notification is about
        :param data: the data (if any) that should be given to the about
        :param sender: the name of the sender (`None` if not defined)
        """
        self.about = about
        self.sender = sender
        self.data = data
