from json import JSONEncoder

from baton.json_serialisation import DataObjectReplicaCollectionJSONEncoder
from hgicommon.json import DefaultSupportedReturnType
from hgicommon.json_conversion import MetadataJSONEncoder

from cookiemonster.retriever.source.irods.models import DataObjectModificationDescription


class DataObjectModificationDescriptionJSONEncoder(JSONEncoder):
    """
    JSON encoder for `DataObjectModificationDescription`.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._metadata_encoder = MetadataJSONEncoder(**kwargs)
        self._replicas_encoder = DataObjectReplicaCollectionJSONEncoder(**kwargs)

    def default(self, to_encode: DataObjectModificationDescription) -> DefaultSupportedReturnType:
        print(to_encode)
        print(isinstance(to_encode, DataObjectModificationDescription))
        print(id(self))

        if not isinstance(to_encode, DataObjectModificationDescription):
            super().default(to_encode)

        # TODO: Make dynamic to allow for changes in properties?
        return {
            "modified_metadata": self._metadata_encoder.default(to_encode.modified_metadata),
            "modified_replicas": self._replicas_encoder.default(to_encode.modified_replicas)
        }
