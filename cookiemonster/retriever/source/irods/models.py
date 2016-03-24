"""
Authors
-------
* Colin Nolan <cn13@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2016 Genome Research Limited
"""
from datetime import datetime
from typing import TypeVar, Generic

from baton import DataObject
from baton.collections import DataObjectReplicaCollection, IrodsMetadata
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
