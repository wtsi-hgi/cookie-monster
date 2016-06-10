"""
Legalese
--------
Copyright (c) 2015 Genome Research Ltd.

Author: Colin Nolan <cn13@sanger.ac.uk>

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
from typing import List, Sequence, Optional, Iterable, Any, Union

from cookiemonster.common.helpers import localise_to_utc
from cookiemonster.common.models import Update, Enrichment
from hgicommon.collections import Metadata


class UpdateCollection(list):
    """
    Collection of `Update` instances. Extends built-in `list`.
    """
    def get_most_recent(self) -> List[Update]:
        """
        Gets the updates in the collection with the most recent timestamp.

        O(n) operation.
        :return: the updates in the collection with the most recent timestamp
        """
        if len(self) == 0:
            raise ValueError("No updates in collection")

        most_recent = [Update("sentinel", datetime.min, Metadata())]
        for update in self:
            assert len(most_recent) > 0
            most_recent_so_far = localise_to_utc(most_recent[0].timestamp)
            timestamp = localise_to_utc(update.timestamp)
            assert timestamp != datetime.min

            if timestamp > most_recent_so_far:
                most_recent.clear()
            if timestamp >= most_recent_so_far:
                most_recent.append(update)

        return most_recent

    def get_entity_updates(self, entity_location: str) -> List[Update]:
        """
        Gets updates relating to the iRODS entity at the given location. The entity may be a collection or data object.

        O(n).
        :param entity_location: the location of the entity (collection or data object) in iRODS
        :return: any updates relating to the entity
        """
        return [update for update in self if update.target == entity_location]


class EnrichmentCollection(Sequence):
    """
    Collection of `Enrichment` instances.
    """
    def __init__(self, seq: Iterable[Enrichment]=None):
        if seq is None:
            seq = {}
        self._data = []     # type: List[Enrichment]
        self.add(seq)

    def __iter__(self) -> Iterable:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __contains__(self, item: Any) -> bool:
        return item in self._data

    def __getitem__(self, index: int) -> Enrichment:
        return self._data[index]

    def add(self, enrichment: Union[Enrichment, Iterable[Enrichment]]):
        """
        Adds an enrichment to this collection.
        :param enrichment: the enrichment to add
        """
        if not isinstance(enrichment, Enrichment):
            for x in enrichment:
                self.add(x)
        else:
            if len(self._data) == 0:
                self._data.append(enrichment)
            else:
                number_of_pre_existing_enrichments = len(self._data)
                i = 0
                while i < number_of_pre_existing_enrichments:
                    prexisting_enrichment = self._data[i]
                    if prexisting_enrichment.timestamp > enrichment.timestamp:
                        self._data.insert(i, enrichment)
                        i = number_of_pre_existing_enrichments
                    elif i == number_of_pre_existing_enrichments - 1:
                        self._data.append(enrichment)
                    i += 1
                assert len(self._data) == number_of_pre_existing_enrichments + 1

    def get_most_recent_from_source(self, source: str) -> Optional[Enrichment]:
        """
        Gets the most recent enrichment from the given source.
        :param source: the source of the enrichment
        :return: the most recent enrichment from the given source, `None` if no enrichments from source
        """
        for enrichment in reversed(self._data):
            if enrichment.source == source:
                return enrichment
        return None

    def get_all_since_enrichment_from_source(self, source: str) -> "EnrichmentCollection":
        """
        Gets all of the enrichments that were added after the most recent enrichment from the given source.
        :param source: the source o
        :return:
        """
        enrichments = EnrichmentCollection()
        for enrichment in reversed(self._data):
            if enrichment.source == source:
                break
            else:
                enrichments.add(enrichment)
        return enrichments

