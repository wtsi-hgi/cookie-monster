import math
from datetime import datetime, timezone
from threading import Semaphore, Thread
from typing import Dict, Iterable, Optional, Sequence
from typing import List

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


class BatonUpdateMapper(BatonCustomObjectMapper[DataObjectModification], UpdateMapper):
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
        all_modifications = []  # type: List[DataObjectModification]
        semaphore = Semaphore(0)
        error = None    # type: Optional(Exception)

        def run_threaded(alias: str):
            updates_query = PreparedSpecificQuery(alias, arguments)
            try:
                modifications = self._get_with_prepared_specific_query(updates_query, zone=self.zone)
                all_modifications.extend(list(modifications))
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

        combined_modifications = BatonUpdateMapper._combine_modifications_for_same_entity(all_modifications)

        # Package modifications into `UpdateCollection`
        updates = BatonUpdateMapper._modifications_to_update_collection(combined_modifications)

        return updates

    def _object_deserialiser(self, object_as_json: dict) -> DataObjectModification:
        metadata_update = MODIFIED_METADATA_ATTRIBUTE_NAME_PROPERTY in object_as_json

        path = "%s/%s" % (object_as_json[MODIFIED_COLLECTION_NAME_PROPERTY],
                          object_as_json[MODIFIED_DATA_NAME_PROPERTY])
        modified_at_as_string = object_as_json[MODIFIED_DATA_TIMESTAMP_PROPERTY] if not metadata_update \
            else object_as_json[MODIFIED_METADATA_TIMESTAMP_PROPERTY]
        modified_at = datetime.fromtimestamp(int(modified_at_as_string), tz=timezone.utc)
        data_object_modification = DataObjectModification(path, modified_at)

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

        return data_object_modification

    @staticmethod
    def _combine_modifications_for_same_entity(modifications: Sequence[DataObjectModification]) \
            -> Sequence[DataObjectModification]:
        """
        Combines modifications descriptions to the same entities into single "merged" description. The merged
        modifications will have the timestamp of the latest update that was merged into them, as discussed in:
        https://github.com/wtsi-hgi/cookie-monster/issues/3#issuecomment-168990482.
        :param modifications: the modifications to combine
        :return: the combined modifications
        """
        combined_modifications_map = dict()   # type: Dict[str, DataObjectModification]

        for modification in modifications:
            id = modification.entity.path

            if id not in combined_modifications_map:
                combined_modifications_map[id] = modification
            else:
                existing_modification = combined_modifications_map[id]

                # Preserve newest timestamp
                if modification.timestamp > existing_modification.timestamp:
                    existing_modification.timestamp = modification.timestamp

                # Merge modification replica
                assert len(modification.modified_replicas) <= 1
                if len(modification.modified_replicas) == 1:
                    updated_replica = modification.modified_replicas.get_all()[0]
                    existing_modification.modified_replicas.add(updated_replica)

                # Merge modification metadata
                assert len(modification.modified_metadata) <= 1
                if len(modification.modified_metadata) == 1:
                    key = list(modification.modified_metadata.keys())[0]
                    assert len(modification.modified_metadata[key]) == 1
                    value = modification.modified_metadata[key].pop()
                    existing_modification.modified_metadata.add(key, value)

        assert len(combined_modifications_map) <= len(modifications)
        return combined_modifications_map.values()

    @staticmethod
    def _modifications_to_update_collection(modifications: Sequence[DataObjectModification]) -> UpdateCollection:
        """
        TODO
        :param modifications:
        :return:
        """
        updates = UpdateCollection()
        for modification in modifications:
            metadata = DataObjectModificationJSONEncoder().default(modification)
            update = Update(modification.entity.path, modification.timestamp, metadata)
            updates.append(update)
        return updates
