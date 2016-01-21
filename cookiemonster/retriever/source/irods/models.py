from baton import DataObject
from baton.collections import DataObjectReplicaCollection, IrodsMetadata
from hgicommon.models import Model


class IrodsEntityModificationDescription(Model):
    """
    Description of modifications to an `IrodsEntity`.
    """
    def __init__(self, path: str):
        self.path = path
        self.modified_metadata = IrodsMetadata()


class DataObjectModificationDescription(IrodsEntityModificationDescription):
    """
    Description of modifications to a `DataObject`.
    """
    def __init__(self, path: str):
        super().__init__(path)
        self.modified_replicas = DataObjectReplicaCollection()