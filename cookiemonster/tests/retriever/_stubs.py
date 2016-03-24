"""
Authors
-------
* Colin Nolan <cn13@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2015, 2016 Genome Research Limited
"""
from datetime import datetime

from cookiemonster.common.collections import UpdateCollection
from cookiemonster.retriever.mappers import UpdateMapper


class StubUpdateMapper(UpdateMapper):
    """
    Stub of `UpdateMapper`.
    """
    def get_all_since(self, since: datetime) -> UpdateCollection:
        return []
