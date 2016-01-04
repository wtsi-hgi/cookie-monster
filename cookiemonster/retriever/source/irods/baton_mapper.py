from datetime import datetime

from baton._baton_mappers import BatonCustomObjectMapper
from baton.models import PreparedSpecificQuery
from baton.types import CustomObjectType
from hgicommon.collections import Metadata

from cookiemonster.common.collections import UpdateCollection
from cookiemonster.common.models import Update
from cookiemonster.retriever.mappers import UpdateMapper
from cookiemonster.retriever.source.irods._baton_constants import BATON_UPDATE_METADATA_ATTRIBUTE_NAME_PROPERTY, \
    BATON_UPDATE_COLLECTION_NAME_PROPERTY, BATON_UPDATE_DATA_OBJECT_NAME_PROPERTY, BATON_UPDATE_HASH_PROPERTY, \
    BATON_UPDATE_DATA_TIMESTAMP_PROPERTY, BATON_UPDATE_METADATA_TIMESTAMP_PROPERTY
from cookiemonster.retriever.source.irods._baton_constants import BATON_UPDATE_METADATA_ATTRIBUTE_VALUE_PROPERTY

_ALIAS = "updates"


class BatonUpdateMapper(BatonCustomObjectMapper[Update], UpdateMapper):
    """
    Retrieves updates from iRODS using baton.
    """
    def get_all_since(self, since: datetime) -> UpdateCollection:
        since_timestamp = since.timestamp()
        until_timestamp = BatonUpdateMapper._get_current_time().timestamp()

        query = PreparedSpecificQuery(_ALIAS, [since_timestamp, until_timestamp, since_timestamp, until_timestamp])
        updates = self._get_with_prepared_specific_query(query)

        # TODO: Merge metadata updates for the same data object

        return UpdateCollection(updates)

    def _object_serialiser(self, object_as_json: dict) -> CustomObjectType:
        metadata = Metadata()
        metadata_update = BATON_UPDATE_METADATA_ATTRIBUTE_NAME_PROPERTY in object_as_json
        if metadata_update:
            key = object_as_json[BATON_UPDATE_METADATA_ATTRIBUTE_NAME_PROPERTY]
            value = object_as_json[BATON_UPDATE_METADATA_ATTRIBUTE_VALUE_PROPERTY]
            metadata[key] = value

        path = "%s/%s" % (object_as_json[BATON_UPDATE_COLLECTION_NAME_PROPERTY],
                       object_as_json[BATON_UPDATE_DATA_OBJECT_NAME_PROPERTY])

        hash = object_as_json[BATON_UPDATE_HASH_PROPERTY] if BATON_UPDATE_HASH_PROPERTY in object_as_json else ""

        modified_at = object_as_json[BATON_UPDATE_DATA_TIMESTAMP_PROPERTY] if not metadata_update \
            else object_as_json[BATON_UPDATE_METADATA_TIMESTAMP_PROPERTY]

        return Update(path, hash, modified_at, metadata)

    @staticmethod
    def _get_current_time() -> datetime:
        """
        Gets the current time. Can be overriden to control environment for testing.
        :return: the current time
        """
        return datetime.now()
