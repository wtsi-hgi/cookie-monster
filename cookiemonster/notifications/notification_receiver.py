"""
Legalese
--------
Copyright (c) 2016 Genome Research Ltd.

Author: Colin Nolan <cn13@sanger.ac.uk>

This file is part of Cookie Monster.

Cookie Monster is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by the
Free Software Foundation; either version 3 of the License, or (at your
option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License along
with this program. If not, see <http://www.gnu.org/licenses/>.
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
