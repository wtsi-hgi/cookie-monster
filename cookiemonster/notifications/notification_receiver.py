"""
Authors
-------
* Colin Nolan <cn13@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2016 Genome Research Limited
"""
import re

from cookiemonster import NotificationReceiver
from cookiemonster.common.resource_accessor import ResourceAccessorContainerRegisteringDataSource, ResourceAccessor


class NotificationReceiverSource(ResourceAccessorContainerRegisteringDataSource):
    """
    Notification receiver source where `NotificationReceiver` are registered from within Python modules within a given
    directory. These modules can be changed on-the-fly.
    """
    # Regex used to determine if a file contains an enrichment loader(s)
    FILE_PATH_MATCH_REGEX = ".*receiver\.py"
    _COMPILED_FILE_PATH_MATCH_REGEX = re.compile(FILE_PATH_MATCH_REGEX)

    def __init__(self, directory_location: str, resource_accessor: ResourceAccessor=None):
        """
        Constructor.
        :param directory_location: the directory in which notification receivers can be sourced from
        :param resource_accessor: resource accessor that notification receivers will be able to use to access resources
        """
        super().__init__(directory_location, NotificationReceiver, resource_accessor)

    def is_data_file(self, file_path: str) -> bool:
        return NotificationReceiverSource._COMPILED_FILE_PATH_MATCH_REGEX.search(file_path)
