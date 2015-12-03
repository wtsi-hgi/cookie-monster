import copy
from queue import PriorityQueue
from typing import List, Optional, Iterable

from cookiemonster.common.models import Cookie, Enrichment
from cookiemonster.processor._models import EnrichmentLoader


class EnrichmentManager:
    """
    TODO
    """
    def __init__(self, enrichment_loaders: Iterable[EnrichmentLoader]=()):
        """
        Default constructor.
        :param enrichment_loaders: TODO
        """
        self.enrichment_loaders = PriorityQueue()
        for enrichment_loader in enrichment_loaders:
            self.enrichment_loaders.put(enrichment_loader)

    def next_enrichment(self, cookie: Cookie) -> Optional[Enrichment]:
        """
        Loads the next set of data not present in the known data given (the "enrichment").

        Returns `None``if all enrichements have already been applied to the cookie.
        :param cookie: the data already known
        :return: the loaded enrichment
        """
        enrichment_loaders = copy.copy(self.enrichment_loaders)

        while not enrichment_loaders.empty():
            enrichment_loader = enrichment_loaders.get()
            if not enrichment_loader.can_enrich(cookie):
                return enrichment_loader.load_enrichment(cookie)

        return None
