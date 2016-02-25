import math
from datetime import datetime, timezone
from threading import Semaphore, Thread
from typing import Dict, Iterable, Optional

from baton._baton_mappers import BatonCustomObjectMapper
from baton.collections import IrodsMetadata
from baton.models import PreparedSpecificQuery, DataObjectReplica
from baton.types import CustomObjectType
from hgicommon.collections import Metadata

from cookiemonster.common.collections import UpdateCollection
from cookiemonster.common.helpers import localise_to_utc
from cookiemonster.common.models import Update
from cookiemonster.retriever.mappers import UpdateMapper
from cookiemonster.retriever.source.irods._constants import MODIFIED_METADATA_ATTRIBUTE_NAME_PROPERTY, \
    MODIFIED_COLLECTION_NAME_PROPERTY, MODIFIED_DATA_NAME_PROPERTY, MODIFIED_DATA_REPLICA_CHECKSUM_PROPERTY, \
    MODIFIED_DATA_TIMESTAMP_PROPERTY, MODIFIED_METADATA_TIMESTAMP_PROPERTY, MODIFIED_DATA_QUERY_ALIAS, \
    MODIFIED_METADATA_ATTRIBUTE_VALUE_PROPERTY, MODIFIED_METADATA_QUERY_ALIAS, MODIFIED_DATA_REPLICA_NUMBER_PROPERTY, \
    MODIFIED_DATA_REPLICA_STATUS_PROPERTY
from cookiemonster.retriever.source.irods.json import DataObjectModificationJSONEncoder, \
    DataObjectModificationJSONDecoder
from cookiemonster.retriever.source.irods.models import DataObjectModification

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_MAX_IRODS_TIMESTAMP = int(math.pow(2, 31)) - 1


class BatonUpdateMapper(BatonCustomObjectMapper[Update], UpdateMapper):
    """
    Retrieves updates from iRODS using baton.
    """
    def __init__(self, baton_binaries_directory: str, zone: str=None):
        super().__init__(baton_binaries_directory)
        self.zone = zone

    def get_all_since(self, since: datetime) -> UpdateCollection:
        # iRODS works with Epoch time therefore ensure `since` is localised as UTC
        since = localise_to_utc(since)

        if since < _EPOCH:
            since = _EPOCH

        since_timestamp = str(int(since.timestamp()))
        until_timestamp = str(_MAX_IRODS_TIMESTAMP)

        arguments = [since_timestamp, until_timestamp]
        aliases = [MODIFIED_DATA_QUERY_ALIAS, MODIFIED_METADATA_QUERY_ALIAS]
        all_updates = []
        semaphore = Semaphore(0)
        error = None    # type: Optional(Exception)

        def run_threaded(alias: str):
            updates_query = PreparedSpecificQuery(alias, arguments)
            try:
                updates = self._get_with_prepared_specific_query(updates_query, zone=self.zone)
                all_updates.extend(list(updates))
            except Exception as e:
                nonlocal error
                error = e
            finally:
                semaphore.release()

        for alias in aliases:
            Thread(target=run_threaded, args=(alias, )).start()
        for _ in range(len(aliases)):
            semaphore.acquire()
            if error is not None:
                raise error

        return BatonUpdateMapper._combine_updates_for_same_entity(all_updates)

    def _object_deserialiser(self, object_as_json: dict) -> CustomObjectType:
        metadata_update = MODIFIED_METADATA_ATTRIBUTE_NAME_PROPERTY in object_as_json

        path = "%s/%s" % (object_as_json[MODIFIED_COLLECTION_NAME_PROPERTY],
                          object_as_json[MODIFIED_DATA_NAME_PROPERTY])
        modified_at_as_string = object_as_json[MODIFIED_DATA_TIMESTAMP_PROPERTY] if not metadata_update \
            else object_as_json[MODIFIED_METADATA_TIMESTAMP_PROPERTY]
        modified_at = datetime.fromtimestamp(int(modified_at_as_string), tz=timezone.utc)
        data_object_modification = DataObjectModification()

        if metadata_update:
            key = object_as_json[MODIFIED_METADATA_ATTRIBUTE_NAME_PROPERTY]
            value = object_as_json[MODIFIED_METADATA_ATTRIBUTE_VALUE_PROPERTY]
            data_object_modification.modified_metadata = IrodsMetadata({key: {value}})
        else:
            replica_number = int(object_as_json[MODIFIED_DATA_REPLICA_NUMBER_PROPERTY])
            checksum = object_as_json[MODIFIED_DATA_REPLICA_CHECKSUM_PROPERTY] \
                if MODIFIED_DATA_REPLICA_CHECKSUM_PROPERTY in object_as_json else ""
            up_to_date = bool(object_as_json[MODIFIED_DATA_REPLICA_STATUS_PROPERTY])

            replica = DataObjectReplica(replica_number, checksum, up_to_date=up_to_date)
            data_object_modification.modified_replicas.add(replica)

        data_object_modification_as_json_dict = DataObjectModificationJSONEncoder().default(data_object_modification)

        return Update(path, modified_at, Metadata(data_object_modification_as_json_dict))

    @staticmethod
    def _combine_updates_for_same_entity(updates: Iterable[Update]) -> UpdateCollection:
        """
        Combines updates for the same entities into single "merged" update entries. The merged update entries will have
        the timestamp of the latest update that was merged into them, as discussed in:
        https://github.com/wtsi-hgi/cookie-monster/issues/3#issuecomment-168990482.
        :param updates: the updates to combine
        :return: the combined updates
        """
        # This whole method is a bit clumsy due to the use of `Metadata` in `Update` leading to a loss of structure of
        # the modifications.
        combined_updates_map = dict()   # type: Dict[str, Update]

        for update in updates:
            update_target = update.target

            if update_target not in combined_updates_map:
                combined_updates_map[update_target] = update
            else:
                existing_update = combined_updates_map[update_target]

                # Convert modifications that the updates are about to models - costly but the structure makes things
                # much easy to work with
                update_modification = DataObjectModificationJSONDecoder().decode_dict(update.metadata)  # type: DataObjectModification
                existing_update_modification = DataObjectModificationJSONDecoder().decode_dict(existing_update.metadata)  # type: DataObjectModification

                # Preserve newest timestamp
                if update.timestamp > existing_update.timestamp:
                    existing_update.timestamp = update.timestamp

                # Merge replica updated
                assert len(update_modification.modified_replicas) <= 1
                if len(update_modification.modified_replicas) == 1:
                    updated_replica = update_modification.modified_replicas.get_all()[0]
                    existing_update_modification.modified_replicas.add(updated_replica)

                # Merge update metadata
                assert len(update_modification.modified_metadata) <= 1
                if len(update_modification.modified_metadata) == 1:
                    key = list(update_modification.modified_metadata.keys())[0]
                    assert len(update_modification.modified_metadata[key]) == 1
                    value = update_modification.modified_metadata[key].pop()
                    existing_update_modification.modified_metadata.add(key, value)

                # Update existing update's metadata
                existing_update_modification_as_json_dict = DataObjectModificationJSONEncoder().default(
                    existing_update_modification)
                existing_update.metadata = Metadata(existing_update_modification_as_json_dict)

        return UpdateCollection(combined_updates_map.values())
