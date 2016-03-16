from datetime import datetime
from functools import total_ordering
from typing import Any, Set, List, Optional

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
    A "Cookie" is a representation of a data object's iteratively enriched metadata.
    """
    def __init__(self, identifier: str):
        self.identifier = identifier
        self.enrichments = []   # type: List[Enrichment]

    def enrich(self, enrichment: Enrichment):
        """
        Enrich this cookie.
        :param enrichment: the enrichment
        """
        if len(self.enrichments) == 0:
            self.enrichments.append(enrichment)
        else:
            number_of_prexisting_enrichments = len(self.enrichments)
            i = 0
            while i < number_of_prexisting_enrichments:
                prexisting_enrichment = self.enrichments[i]
                if prexisting_enrichment.timestamp > enrichment.timestamp:
                    self.enrichments.insert(i, enrichment)
                    i = number_of_prexisting_enrichments
                elif i == number_of_prexisting_enrichments - 1:
                    self.enrichments.append(enrichment)
                i += 1
            assert len(self.enrichments) == number_of_prexisting_enrichments + 1

    def get_most_recent_enrichment_from_source(self, source: str) -> Optional[Enrichment]:
        """
        Gets the most recent enrichment from the given source.
        :param source: the source of the enrichment
        :return: the most recent enrichment from the given source, `None` if no enrichments from source
        """
        for enrichment in reversed(self.enrichments):
            if enrichment.source == source:
                return enrichment
        return None

    def get_enrichment_sources(self) -> Set[str]:
        """
        Fetches the distinct enrichment sources for which metadata exists.
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
