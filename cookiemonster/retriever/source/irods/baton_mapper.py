from datetime import datetime
from typing import List, Any

from cookiemonster.common.collections import UpdateCollection
from cookiemonster.retriever._models import QueryResult
from cookiemonster.retriever.source.irods.irods_config import IrodsConfig
from cookiemonster.retriever.mappers import UpdateMapper


class BatonUpdateMapper(UpdateMapper):
    """
    Retrieves updates from iRODS using baton.
    """
    def __init__(self, irods_config: IrodsConfig):
        """
        Constructor.
        :param irods_config: the configuration iRODS requires to connect to iRODS.
        """
        self._irods_config = irods_config

    def get_all_since(self, since: datetime) -> QueryResult:
        raise NotImplementedError()

    @staticmethod
    def _convert_to_models(updates: List[Any]) -> UpdateCollection:
        """
        Converts a given list of file update entries (in the form of the JSON returned by iRODS) into
        `UpdateCollection`.
        :param updates: the file update entries ni the form of the JSON returned by iRODS
        :return: a `UpdateCollection` created from the given entries
        """
        raise NotImplementedError()
