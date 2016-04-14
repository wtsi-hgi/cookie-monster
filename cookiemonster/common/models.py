"""
Legalese
--------
Copyright (c) 2015, 2016 Genome Research Ltd.

Authors:
* Colin Nolan <cn13@sanger.ac.uk>
* Christopher Harrison <ch12@sanger.ac.uk>

This file is part of Cookie Monster.

Cookie Monster is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by the
Free Software Foundation; either version 3 of the License, or (at your
option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License along
with this program. If not, see <http://www.gnu.org/licenses/>.
"""
from datetime import datetime
from functools import total_ordering
from typing import Any, Set, List, Optional, Union

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


class EnrichmentDiff(Model):
    """
    Metadata enrichment difference model
    """
    def __init__(self, basis:Enrichment, comparator:Enrichment, keys:Optional[List[str]] = None):
        if basis.source != comparator.source:
            # FIXME? This isn't really a TypeError
            raise TypeError('Basis and comparator are from different enrichment sources')

        self.source = basis.source
        self.timestamp = comparator.timestamp

        # Metadata diffs
        self.additions = Metadata()
        self.deletions = Metadata()
        self._diff(basis.metadata, comparator.metadata, keys)

    def _diff(self, base:Metadata, comp:Metadata, keys:Optional[List[str]] = None):
        """
        Calculate the difference between two metadata dictionaries,
        optionally specific to a list of keys

        @param   base  Metadata dictionary that forms the basis of comparison
        @param   comp  Metadata dictionary to check against basis
        @param   keys  Interested keys (optional; check all if omitted)
        """
        # Common keys
        for common in set(comp.keys()).intersection(set(base.keys())):
            if not keys or common in keys:
                if base[common] != comp[common]:
                    self.additions[common] = comp[common]
                    self.deletions[common] = base[common]

        # New keys
        for added in (comp.keys() - base.keys()):
            if not keys or added in keys:
                self.additions[added] = comp[added]

        # Deleted keys
        for deleted in (base.keys() - comp.keys()):
            if not keys or deleted in keys:
                self.deletions[deleted] = base[deleted]

    def is_different(self) -> bool:
        """
        Is this a non-trivial diff?
        """
        return bool(self.additions or self.deletions)


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
            number_of_pre_existing_enrichments = len(self.enrichments)
            i = 0
            while i < number_of_pre_existing_enrichments:
                prexisting_enrichment = self.enrichments[i]
                if prexisting_enrichment.timestamp > enrichment.timestamp:
                    self.enrichments.insert(i, enrichment)
                    i = number_of_pre_existing_enrichments
                elif i == number_of_pre_existing_enrichments - 1:
                    self.enrichments.append(enrichment)
                i += 1
            assert len(self.enrichments) == number_of_pre_existing_enrichments + 1

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

    def get_enrichment_changes_from_source( self, source: str, keys: Union[None, str, List[str]]=None,
                                            since: Optional[datetime]=None) -> List[EnrichmentDiff]:
        """
        Get the running changes in metadata from an enrichment source
        and, optional, key/list of keys based from the first known
        enrichment

        @param   source  Enrichment source
        @param   keys    Metadata key(s) to check (optional; check all if omitted)
        @param   since   Time from which to check for changes (optional; check all if omitted)
        @return  List of differences

        TODO? Do we need a `before` parameter, as well...
        """
        first_comparator_index = 1
        output = []

        if since:
            for enrichment in self.enrichments[1:]:
                if enrichment.timestamp < since:
                    first_comparator_index += 1
                else:
                    # Enrichment list should be ordered by timestamp
                    break

        total = len(self.enrichments)
        if total <= first_comparator_index:
            return output

        # Normalise single key
        if isinstance(keys, str):
            keys = [keys]

        for i in range(first_comparator_index, total):
            diff = EnrichmentDiff(self.enrichments[i - 1], self.enrichments[i], keys)
            if diff.is_different():
                output.append(diff)

        return output

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
