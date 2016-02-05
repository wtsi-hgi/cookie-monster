from datetime import datetime
from functools import total_ordering
from typing import Any, Union, Set, List

from hgicommon.collections import Metadata
from hgicommon.models import Model


class Update(Model):
    """
    Model of a file update.
    """
    def __init__(self, target: str, timestamp: datetime, metadata: Metadata=None):
        self.target = target
        self.timestamp = timestamp
        self.metadata = metadata if metadata is not None else Metadata()


@total_ordering
class Enrichment(Model):
    """
    Metadata enrichment model
    """
    def __init__(self, source: str, timestamp: datetime, metadata: Metadata):
        self.source = source
        self.timestamp = timestamp
        self.metadata = metadata

    def __lt__(self, other):
        return self.timestamp < other.timestamp


class Cookie(Model):
    """
    A "Cookie" is a representation of a file's iteratively enriched metadata.
    """
    def __init__(self, path: str):
        self.path = path
        self.enrichments = []   # type: List[Enrichment]

    def enrich(self, enrichment: Enrichment):
        """
        Enrich this cookie.
        :param enrichment: the enrichment
        """
        self.enrichments.append(enrichment)

    def get_metadata_by_source(self, source: str, key: str, default: Any=None):
        """
        Fetch the latest existing metadata by source and key.
        :param source enrichment source
        :param key attribute name
        :param default default value, if key doesn't exist
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

    def get_metadata_sources(self) -> Set[str]:
        """
        Fetch the distinct enrichment sources for which metadata exists.
        :return: the enrichment sources
        """
        return {enrichment.source for enrichment in self.enrichments}


class Notification(Model):
    """
    A model of a notification that can be sent to a `NotificationReceiver`.
    """
    def __init__(self, about: str, data: Any=None, sender: str=None):
        self.about = about
        self.sender = sender
        self.data = data
