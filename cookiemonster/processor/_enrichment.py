import logging
import re
import traceback
from queue import PriorityQueue
from typing import Optional, Iterable

from hgicommon.data_source import RegisteringDataSource

from cookiemonster.common.models import Cookie, Enrichment
from cookiemonster.processor.models import EnrichmentLoader


class EnrichmentManager:
    """
    Manages the enrichment of cookies.
    """
    def __init__(self, enrichment_loaders: Iterable[EnrichmentLoader]=()):
        """
        Constructor.
        :param enrichment_loaders: the source of enrichment loaders
        """
        self.enrichment_loaders = enrichment_loaders

    def next_enrichment(self, cookie: Cookie) -> Optional[Enrichment]:
        """
        Loads the next set of data not present in the known data given (the "enrichment").

        Returns `None``if all enrichments have already been applied to the cookie.
        :param cookie: the data already known
        :return: the loaded enrichment
        """
        enrichment_loaders_priority_queue = PriorityQueue()
        for enrichment_loader in self.enrichment_loaders:
            enrichment_loaders_priority_queue.put(enrichment_loader)

        while not enrichment_loaders_priority_queue.empty():
            enrichment_loader = enrichment_loaders_priority_queue.get()

            enrich = False
            try:
                enrich = enrichment_loader.can_enrich(cookie)
            except Exception as e:
                logging.error("Error checking if enrichment can be applied to cookie; Enrichment loader: %s;"
                              "Target Cookie: %s; Error: %s" % (enrichment_loader, cookie.identifier, e))

            if enrich:
                try:
                    return enrichment_loader.load_enrichment(cookie)
                except Exception:
                    logging.error("Error loading enrichment; Enrichment loader: %s; Target Cookie: %s; Error: %s"
                                  % (enrichment_loader, cookie.identifier, traceback.format_exc()))

        return None


class EnrichmentLoaderSource(RegisteringDataSource):
    """
    Enrichment loader source where enrichment loaders are registered from within Python modules within a given
    directory. These modules can be changed on-the-fly.
    """
    # Regex used to determine if a file contains an enrichment loader(s)
    FILE_PATH_MATCH_REGEX = ".*loader\.py"
    _COMPILED_FILE_PATH_MATCH_REGEX = re.compile(FILE_PATH_MATCH_REGEX)

    def __init__(self, directory_location: str):
        """
        Constructor.
        :param directory_location: the directory in which enrichment loaders can be sourced from
        """
        super().__init__(directory_location, EnrichmentLoader)

    def is_data_file(self, file_path: str) -> bool:
        return EnrichmentLoaderSource._COMPILED_FILE_PATH_MATCH_REGEX.search(file_path)
