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
    def __init__(self, entity: _EntityType=None, timestamp: datetime=None, modified_metadata: IrodsMetadata=None):
        self.entity = entity
        self.timestamp = timestamp
        self.modified_metadata = modified_metadata if modified_metadata is not None else IrodsMetadata()


class DataObjectModification(IrodsEntityModification[DataObject]):
    """
    Description of modification to a `DataObject`.
    """
    def __init__(self, path: str=None, timestamp: datetime=None, modified_metadata: IrodsMetadata=None,
                 modified_replicas: DataObjectReplicaCollection=None):
        super().__init__(DataObject(path), timestamp, modified_metadata)
        self.modified_replicas = modified_replicas if modified_replicas is not None else DataObjectReplicaCollection()
