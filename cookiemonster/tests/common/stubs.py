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
