from datetime import datetime
from typing import List, Dict

from cookiemonster.common.models import FileUpdateCollection
from cookiemonster.dataretriever._baton.irods_config import IrodsConfig
from cookiemonster.dataretriever._retriever import FileUpdateRetriever, QueryResult


class BatonFileUpdateRetriever(FileUpdateRetriever):
    """
    TODO
    """
    def __init__(self, irods_config: IrodsConfig):
        """
        Constructor.
        :param irods_config: the configuration baton requires to connect to iRODs.
        """
        self._irods_config = irods_config

    def query_for_all_file_updates_since(self, since: datetime) -> QueryResult:
        raise NotImplementedError()

    @staticmethod
    def _convert_to_models(file_update_entires: List[Dict[str, str, str]]) -> FileUpdateCollection:
        """
        Converts a given list of file update entries (in the form of the JSON returned by baton) into
        `FileUpdateCollection`.
        :param file_update_entires: the file update entries ni the form of the JSON returned by baton
        :return: a `FileUpdateCollection` created from the given entries
        """
        raise NotImplementedError()
