from typing import List, Optional

from cookiemonster.common.models import CookieCrumbs, Cookie, Enrichment
from cookiemonster.processor._models import EnrichmentLoader


class EnrichmentManager:
    """
    TODO
    """
    def __init__(self, enrichment_loaders: List[EnrichmentLoader]=()):
        """
        Default constructor.
        :param enrichment_loaders: ordered list of data loaders where the first loader in the list is used first
        """
        self.data_loaders = []  # type: List[EnrichmentLoader]
        for data_loader in enrichment_loaders:
            self.data_loaders.append(data_loader)

    def next_enrichment(self, cookie: Cookie) -> Optional[Enrichment]:
        """
        Loads the next set of data not present in the known data given.

        None if all data that the data loaders can generate is known.
        :param cookie: the data already known
        :return: the loaded enrichment
        """
        for enrichment_loader in self.data_loaders:
            if not enrichment_loader.is_already_known(cookie):
                return enrichment_loader.load(cookie)

        return None
