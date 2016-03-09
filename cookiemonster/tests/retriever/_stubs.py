from datetime import datetime

from cookiemonster.common.collections import UpdateCollection
from cookiemonster.retriever.mappers import UpdateMapper


class StubUpdateMapper(UpdateMapper):
    """
    Stub of `UpdateMapper`.
    """
    def get_all_since(self, since: datetime) -> UpdateCollection:
        return []
