import asyncio
from datetime import datetime, timezone
from typing import Dict, Iterable

import pytz
from baton._baton_mappers import BatonCustomObjectMapper
from baton.models import PreparedSpecificQuery
from baton.types import CustomObjectType
from hgicommon.collections import Metadata

from cookiemonster.common.collections import UpdateCollection
from cookiemonster.common.models import Update
from cookiemonster.retriever.mappers import UpdateMapper
from cookiemonster.retriever.source.irods._constants import UPDATE_METADATA_ATTRIBUTE_NAME_PROPERTY, \
    UPDATE_COLLECTION_NAME_PROPERTY, UPDATE_DATA_OBJECT_NAME_PROPERTY, UPDATE_HASH_PROPERTY, \
    UPDATE_DATA_TIMESTAMP_PROPERTY, UPDATE_METADATA_TIMESTAMP_PROPERTY, DATA_UPDATES_QUERY_ALIAS, \
    UPDATE_METADATA_ATTRIBUTE_VALUE_PROPERTY, METADATA_UPDATES_QUERY_ALIAS

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

_HASH_METADATA_KEY = "hash"


class BatonUpdateMapper(BatonCustomObjectMapper[Update], UpdateMapper):
    """
    Retrieves updates from iRODS using baton.
    """
    def get_all_since(self, since: datetime) -> UpdateCollection:
        # iRODS works with Epoch time therefore ensure `since` is localised as UTC
        if since.tzinfo is None:
            since = pytz.utc.localize(since)
        else:
            since = since.astimezone(pytz.utc)

        if since < _EPOCH:
            since = _EPOCH

        since_timestamp = str(int(since.timestamp()))
        until_timestamp = str(int(datetime.max.timestamp()))

        # "since" appears to need to comprise of 11 digits. Leading zeros can be used to pad the length. However, if it
        # has too many leading zeros, the number appears to be treated as "0". If it does not have enough, no entries
        # are returned
        since_timestamp = since_timestamp.zfill(11)

        arguments = [since_timestamp, until_timestamp, since_timestamp, until_timestamp]

        updates = []

        async def _run_query(query: PreparedSpecificQuery) -> Iterable[Update]:
            return self._get_with_prepared_specific_query(updates_query)

        aliases = [DATA_UPDATES_QUERY_ALIAS, METADATA_UPDATES_QUERY_ALIAS]
        async for alias in aliases:
            updates_query = PreparedSpecificQuery(alias, arguments)
            updates.extend(list(_run_query(updates_query)))


        return BatonUpdateMapper._combine_updates_for_same_entity(updates)

    def _object_serialiser(self, object_as_json: dict) -> CustomObjectType:
        metadata = Metadata()
        metadata_update = UPDATE_METADATA_ATTRIBUTE_NAME_PROPERTY in object_as_json
        if metadata_update:
            key = object_as_json[UPDATE_METADATA_ATTRIBUTE_NAME_PROPERTY]
            value = object_as_json[UPDATE_METADATA_ATTRIBUTE_VALUE_PROPERTY]
            metadata[key] = value
        else:
            hash = object_as_json[UPDATE_HASH_PROPERTY] if UPDATE_HASH_PROPERTY in object_as_json else ""
            metadata[_HASH_METADATA_KEY] = hash

        path = "%s/%s" % (object_as_json[UPDATE_COLLECTION_NAME_PROPERTY],
                       object_as_json[UPDATE_DATA_OBJECT_NAME_PROPERTY])

        modified_at_as_string = object_as_json[UPDATE_DATA_TIMESTAMP_PROPERTY] if not metadata_update \
            else object_as_json[UPDATE_METADATA_TIMESTAMP_PROPERTY]
        modified_at = datetime.fromtimestamp(int(modified_at_as_string), tz=timezone.utc)

        return Update(path, modified_at, metadata)

    @staticmethod
    def _combine_updates_for_same_entity(updates: Iterable[Update]) -> UpdateCollection:
        """
        Combines updates for the same entities into single "merged" update entries. The merged update entries will have
        the timestamp of the latest update that was merged into them.

        Implemented due to: https://github.com/wtsi-hgi/cookie-monster/issues/3#issuecomment-168990482.
        :param updates: the updates to combine
        :return: the combined updates
        """
        combined_updates_map = dict()   # type: Dict[str, Update]

        for update in updates:
            target = update.target

            if target not in combined_updates_map:
                combined_updates_map[target] = update
            else:
                existing_update = combined_updates_map[target]

                # Preserve newest timestamp
                if update.timestamp > existing_update.timestamp:
                    existing_update.timestamp = update.timestamp

                # Merge metadata together
                for key, value in update.metadata.items():
                    # Assumed iRODS always merges multiple updates to the same thing
                    assert key not in existing_update.metadata
                    existing_update.metadata[key] = value

        return UpdateCollection(combined_updates_map.values())
