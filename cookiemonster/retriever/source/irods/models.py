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
from datetime import datetime
from typing import TypeVar, Generic

from baton.collections import DataObjectReplicaCollection, IrodsMetadata
from baton.models import DataObject
from hgicommon.models import Model

_EntityType = TypeVar("EntityType")


class IrodsEntityModification(Generic[_EntityType], Model):
    """
    Description of modification to an `IrodsEntity`.
    """
    def __init__(self, modified_metadata: IrodsMetadata=None):
        self.modified_metadata = modified_metadata if modified_metadata is not None else IrodsMetadata()


class DataObjectModification(IrodsEntityModification[DataObject]):
    """
    Description of modification to a `DataObject`.
    """
    def __init__(self, modified_metadata: IrodsMetadata=None, modified_replicas: DataObjectReplicaCollection=None):
        super().__init__(modified_metadata)
        self.modified_replicas = modified_replicas if modified_replicas is not None else DataObjectReplicaCollection()


class IrodsEntityUpdate(Generic[_EntityType], Model):
    """
    Description of an update to an entity in iRODS.
    """
    def __init__(self, entity: _EntityType, timestamp: datetime, modification: _EntityType=None):
        self.entity = entity
        self.timestamp = timestamp
        self.modification = modification


class DataObjectUpdate(IrodsEntityUpdate[DataObject]):
    """
    Description of an update to an data object in iRODS.
    """
    def __init__(self, path: str, timestamp: datetime, modification: DataObjectModification=None):
        modification = modification if modification is not None else DataObjectModification()
        super().__init__(DataObject(path), timestamp, modification)
