from baton.collections import DataObjectReplicaCollection, IrodsMetadata
from hgicommon.models import Model


class ModificationDescription(Model):
    """
    TODO
    """
    def __init__(self, path: str):
        self.path = path


class DataObjectModificationDescription(ModificationDescription):
    """
    TODO
    """
    def __init__(self, path: str):
        super().__init__(path)
        self.modified_metadata = IrodsMetadata()
        self.modified_replicas = DataObjectReplicaCollection()
