"""
Authors
-------
* Colin Nolan <cn13@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2016 Genome Research Limited
"""
from cookiemonster.common.resource_accessor import ResourceAccessorContainerRegisteringDataSource, ResourceAccessor, \
    ResourceAccessorContainer


class StubResourceAccessor(ResourceAccessor):
    """
    Stub `ResourceAccessor`.
    """


class StubResourceAccessorContainer(ResourceAccessorContainer):
    """
    Stub `ResourceAccessorContainer`.
    """


class StubResourceAccessorContainerRegisteringDataSource(ResourceAccessorContainerRegisteringDataSource):
    """
    Stub `ResourceAccessorContainerRegisteringDataSource`.
    """
    def is_data_file(self, file_path: str) -> bool:
        return True
