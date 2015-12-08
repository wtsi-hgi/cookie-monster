from datetime import datetime
from typing import List

from hgicommon.collections import Metadata

from cookiemonster.common.models import Update


class FileUpdateCollection(list):
    """
    Collection of `Update` instances. Extends built-in `list`.
    """
    def get_most_recent(self) -> List[Update]:
        """
        Gets the file updates in the collection with the most recent timestamp.

        O(n) operation.
        :return: the file updates in the collection with the most recent timestamp
        """
        if len(self) == 0:
            raise ValueError("No file updates in collection")

        most_recent = [Update("", "", datetime.min, Metadata())]
        for file_update in self:
            assert len(most_recent) > 0
            most_recent_so_far = most_recent[0].timestamp
            if file_update.timestamp > most_recent_so_far:
                most_recent.clear()
            if file_update.timestamp >= most_recent_so_far:
                most_recent.append(file_update)

        return most_recent


# TODO: Is this still required?
class EnrichmentCollection(list):
    """
    Collection of `Enrichment` instances; extends `list`
    """
    pass
