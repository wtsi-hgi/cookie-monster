import re
from queue import PriorityQueue
from typing import Optional

from hgicommon.data_source import DataSource, ListDataSource, RegisteringDataSource

from cookiemonster.common.models import Cookie, Enrichment
from cookiemonster.processor.models import EnrichmentLoader


class EnrichmentManager:
    """
    Manages the enrichment of cookies.
    """
    def __init__(self, enrichment_loader_source: DataSource[EnrichmentLoader]=ListDataSource([])):
        """
        Constructor.
        :param enrichment_loader_source: the source of enrichment loaders
        """
        self.enrichment_loader_source = enrichment_loader_source

    def next_enrichment(self, cookie: Cookie) -> Optional[Enrichment]:
        """
        Loads the next set of data not present in the known data given (the "enrichment").

        Returns `None``if all enrichements have already been applied to the cookie.
        :param cookie: the data already known
        :return: the loaded enrichment
        """
        enrichment_loaders = self.enrichment_loader_source.get_all()
        enrichment_loaders_priority_queue = PriorityQueue()

        for enrichment_loader in enrichment_loaders:
            enrichment_loaders_priority_queue.put(enrichment_loader)

        while not enrichment_loaders_priority_queue.empty():
            enrichment_loader = enrichment_loaders_priority_queue.get()
            if enrichment_loader.can_enrich(cookie):
                return enrichment_loader.load_enrichment(cookie)

        return None


class EnrichmentLoaderSource(RegisteringDataSource):
    """
    Enrichment loader source where enrichment loaders are registered from within Python modules within a given
    directory. These modules can be changed on-the-fly.
    """
    # Regex used to determine if a file contains an enrichment loader(s)
    FILE_PATH_MATCH_REGEX = ".*loader\.py"

    def __init__(self, directory_location: str):
        """
        Constructor.
        :param directory_location: the directory in which enrichment loaders can be sourced from
        """
        super().__init__(directory_location, EnrichmentLoader)

    # Compiled `FILE_PATH_MATCH_REGEX`
    _compiled_file_path_match_regex = re.compile(FILE_PATH_MATCH_REGEX)

    def is_data_file(self, file_path: str) -> bool:
        return EnrichmentLoaderSource._compiled_file_path_match_regex.search(file_path)
