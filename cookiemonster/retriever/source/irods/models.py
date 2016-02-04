from baton.collections import DataObjectReplicaCollection, IrodsMetadata
from hgicommon.models import Model


class IrodsEntityModification(Model):
    """
    Description of modification to an `IrodsEntity`.
    """
    def __init__(self, modified_metadata: IrodsMetadata=None):
        self.modified_metadata = modified_metadata if modified_metadata is not None else IrodsMetadata()


class DataObjectModification(IrodsEntityModification):
    """
    Description of modification to a `DataObject`.
    """
    def __init__(self, modified_metadata: IrodsMetadata=None, modified_replicas: DataObjectReplicaCollection=None):
        super().__init__(modified_metadata)
        self.modified_replicas = modified_replicas if modified_replicas is not None else DataObjectReplicaCollection()
