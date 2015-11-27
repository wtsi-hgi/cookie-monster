from datetime import datetime
from typing import List, Any

from cookiemonster.common.collections import FileUpdateCollection
from cookiemonster.retriever._retriever import FileUpdateRetriever, QueryResult
from cookiemonster.retriever.irods.irods_config import IrodsConfig


class BatonFileUpdateRetriever(FileUpdateRetriever):
    """
    Retrieves file updates from iRODS using baton.
    """
    def __init__(self, irods_config: IrodsConfig):
        """
        Constructor.
        :param irods_config: the configuration iRODS requires to connect to iRODS.
        """
        self._irods_config = irods_config

    def query_for_all_file_updates_since(self, since: datetime) -> QueryResult:
        raise NotImplementedError()

    @staticmethod
    def _convert_to_models(file_update_entries: List[Any]) -> FileUpdateCollection:
        """
        Converts a given list of file update entries (in the form of the JSON returned by iRODS) into
        `FileUpdateCollection`.
        :param file_update_entries: the file update entries ni the form of the JSON returned by iRODS
        :return: a `FileUpdateCollection` created from the given entries
        """
        raise NotImplementedError()
