from json import JSONEncoder

from baton.json_serialisation import DataObjectReplicaCollectionJSONEncoder
from hgicommon.json import DefaultSupportedReturnType

from cookiemonster.retriever.source.irods.models import DataObjectModificationDescription, \
    IrodsEntityModificationDescription


class IrodsEntityModificationDescriptionJSONEncoder(JSONEncoder):
    """
    JSON encoder for `IrodsEntityModificationDescription`.
    """
    def default(self, to_encode: IrodsEntityModificationDescription) -> DefaultSupportedReturnType:
        if not isinstance(to_encode, IrodsEntityModificationDescription):
            JSONEncoder.default(self, to_encode)

        return {
            "modified_metadata": dict(to_encode.modified_metadata)
        }


class DataObjectModificationDescriptionJSONEncoder(IrodsEntityModificationDescriptionJSONEncoder):
    """
    JSON encoder for `DataObjectModificationDescription`.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._replicas_encoder = DataObjectReplicaCollectionJSONEncoder(**kwargs)

    def default(self, to_encode: DataObjectModificationDescription) -> DefaultSupportedReturnType:
        if not isinstance(to_encode, DataObjectModificationDescription):
            JSONEncoder.default(self, to_encode)

        encoded_replicas = self._replicas_encoder.default(to_encode.modified_replicas)  # type: list
        # Remove properties of replica that are not know via the update retrieve
        for encoded_replica in encoded_replicas:
            del encoded_replica["host"]
            del encoded_replica["resource_name"]

        encoded = super().default(to_encode)
        encoded.update({
            "modified_replicas": encoded_replicas
        })
        return encoded
