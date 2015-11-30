from typing import List, Optional

from cookiemonster.common.models import CookieCrumbs, Cookie, Enrichment
from cookiemonster.processor._models import DataLoader


# TODO: Requires a better name...
class DataLoaderManager:
    """
    TODO
    """
    def __init__(self, data_loaders: List[DataLoader]=()):
        """
        Default constructor.
        :param data_loaders: ordered list of data loaders where the first loader in the list is used first
        """
        self.data_loaders = []  # type: List[DataLoader]
        for data_loader in data_loaders:
            self.data_loaders.append(data_loader)

    def load_next(self, cookie: Cookie) -> Optional[Enrichment]:
        """
        Loads the next set of data not present in the known data given.

        None if all data that the data loaders can generate is known.
        :param cookie: the data already known
        :return: the loaded data
        """
        for data_loader in self.data_loaders:
            if not data_loader.is_already_known(cookie):
                return data_loader.load(cookie)

        return None
