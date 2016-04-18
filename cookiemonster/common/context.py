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
from abc import ABCMeta
from typing import Optional
from typing import TypeVar, Generic, Iterable

from hgicommon.data_source import RegisteringDataSource
from hgicommon.data_source.common import DataSourceType
from hgicommon.models import Model


class Context(metaclass=ABCMeta):
    """
    Object through which resources can be accessed. Implementations dictate the resources that are made available and
    the methods through which they are accessed.
    """

ContextType = TypeVar("ContextType", bound=Context)


class ContextContainer(Generic[ContextType], Model, metaclass=ABCMeta):
    """
    Object that has access to a context.
    """
    def __init__(self, *args, **kwargs):
        """
        Constructor.
        """
        super().__init__(*args, **kwargs)
        self.context = None  # type: Optional[ContextType]


class ContextContainerRegisteringDataSource(RegisteringDataSource, metaclass=ABCMeta):
    """
    Registering data source of `ContextContainer` instances.
    """
    def __init__(self, directory_location: str, data_type: type, context: Context=None):
        """
        Constructor.
        :param directory_location: the location to monitor for registering data
        :param data_type: the type of data that will be registered (should be a subclass of `ContextContainer`)
        :param context: the context that should injected into the containers
        """
        super().__init__(directory_location, data_type)
        self.context = context

    def extract_data_from_file(self, file_path: str) -> Iterable[DataSourceType]:
        context_containers = super().extract_data_from_file(file_path)
        if self.context is not None:
            for context_container in context_containers:
                context_container.context = self.context
        return context_containers
