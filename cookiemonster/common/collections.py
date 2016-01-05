from datetime import datetime
from typing import List

import pytz
from hgicommon.collections import Metadata

from cookiemonster.common.models import Update


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
            most_recent_so_far = UpdateCollection._localise_to_utc(most_recent[0].timestamp)
            timestamp = UpdateCollection._localise_to_utc(update.timestamp)
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

    @staticmethod
    def _localise_to_utc(timestamp: datetime) -> datetime:
        """
        Localise the given timestamp to UTC.
        :param timestamp: the timestamp to localise
        :return: the timestamp localised to UTC
        """
        if timestamp.tzinfo is None:
            return pytz.utc.localize(timestamp)
        else:
            return timestamp.astimezone(pytz.utc)
