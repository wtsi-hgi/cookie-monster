"""
Legalese
--------
Copyright (c) 2016 Genome Research Ltd.

Author: Colin Nolan <cn13@sanger.ac.uk>

This file is part of Cookie Monster.

Cookie Monster is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by the
Free Software Foundation; either version 3 of the License, or (at your
option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License along
with this program. If not, see <http://www.gnu.org/licenses/>.
"""
import logging
import math
import time
from datetime import datetime, timezone
from threading import Semaphore, Thread
from typing import Dict, List, Optional, Sequence

from baton._baton.baton_custom_object_mappers import BatonCustomObjectMapper
from baton.collections import IrodsMetadata
from baton.models import PreparedSpecificQuery, DataObjectReplica
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
from cookiemonster.retriever.source.irods.json import DataObjectModificationJSONEncoder
from cookiemonster.retriever.source.irods.models import DataObjectUpdate

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_MAX_IRODS_TIMESTAMP = int(math.pow(2, 31)) - 1


class BatonUpdateMapper(BatonCustomObjectMapper[DataObjectUpdate], UpdateMapper):
    """
    Retrieves updates from iRODS using baton.
    """
    DATA_OBJECT_MODIFICATION_JSON_ENCODER = DataObjectModificationJSONEncoder()

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
        all_updates = []  # type: List[DataObjectUpdate]
        semaphore = Semaphore(0)
        error = None    # type: Optional(Exception)

        def run_threaded(alias: str):
            updates_query = PreparedSpecificQuery(alias, arguments)
            try:
                started_at = time.monotonic()
                updates = self._get_with_prepared_specific_query(updates_query, zone=self.zone)
                logging.info("Took %f seconds (wall time) to get and then parse %d iRODS updates using `%s` query"
                             % (time.monotonic() - started_at, len(updates), alias))
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

        started_at = time.monotonic()
        combined_modifications = BatonUpdateMapper._combine_updates_for_same_entity(all_updates)
        logging.info("Took %f seconds (wall time) to merge %d updates related to %d data objects"
                     % (time.monotonic() - started_at, len(all_updates), len(combined_modifications)))

        # Package modifications into `UpdateCollection`
        started_at = time.monotonic()
        updates = BatonUpdateMapper._data_object_updates_to_generic_update_collection(combined_modifications)
        logging.info("Took %f seconds (wall time) to convert %d updates to generic updates that can be stored in the "
                     "knowledge base" % (time.monotonic() - started_at, len(combined_modifications)))

        return updates

    def _object_deserialiser(self, object_as_json: dict) -> DataObjectUpdate:
        metadata_update = MODIFIED_METADATA_ATTRIBUTE_NAME_PROPERTY in object_as_json

        path = "%s/%s" % (object_as_json[MODIFIED_COLLECTION_NAME_PROPERTY],
                          object_as_json[MODIFIED_DATA_NAME_PROPERTY])
        modified_at_as_string = object_as_json[MODIFIED_DATA_TIMESTAMP_PROPERTY] if not metadata_update \
            else object_as_json[MODIFIED_METADATA_TIMESTAMP_PROPERTY]
        modified_at = datetime.fromtimestamp(int(modified_at_as_string), tz=timezone.utc)
        data_object_update = DataObjectUpdate(path, modified_at)

        if metadata_update:
            key = object_as_json[MODIFIED_METADATA_ATTRIBUTE_NAME_PROPERTY]
            value = object_as_json[MODIFIED_METADATA_ATTRIBUTE_VALUE_PROPERTY]
            data_object_update.modification.modified_metadata = IrodsMetadata({key: {value}})
        else:
            replica_number = int(object_as_json[MODIFIED_DATA_REPLICA_NUMBER_PROPERTY])
            checksum = object_as_json[MODIFIED_DATA_REPLICA_CHECKSUM_PROPERTY] \
                if MODIFIED_DATA_REPLICA_CHECKSUM_PROPERTY in object_as_json else ""
            up_to_date = bool(object_as_json[MODIFIED_DATA_REPLICA_STATUS_PROPERTY])

            replica = DataObjectReplica(replica_number, checksum, up_to_date=up_to_date)
            data_object_update.modification.modified_replicas.add(replica)

        return data_object_update

    @staticmethod
    def _combine_updates_for_same_entity(updates: Sequence[DataObjectUpdate]) -> Sequence[DataObjectUpdate]:
        """
        Combines update to the same entities into single "merged" description. The merged updates will have the
        timestamp of the most recent update that was merged into them, as discussed in:
        https://github.com/wtsi-hgi/cookie-monster/issues/3#issuecomment-168990482.
        :param updates: the updates to combine
        :return: the combined updates
        """
        combined_updates_map = dict()   # type: Dict[str, DataObjectUpdate]

        for update in updates:
            # Assert that each update in iRODS relates to only one modification (hence the need to merge)
            assert len(update.modification.modified_replicas) <= 1
            assert len(update.modification.modified_metadata) <= 1

            identifier = update.entity.path

            if identifier not in combined_updates_map:
                combined_updates_map[identifier] = update
            else:
                existing_update = combined_updates_map[identifier]

                if update.timestamp > existing_update.timestamp:
                    # Preserve newest timestamp
                    existing_update.timestamp = update.timestamp

                if len(update.modification.modified_replicas) == 1:
                    # Merge modification replica
                    updated_replica = update.modification.modified_replicas.get_all()[0]
                    existing_update.modification.modified_replicas.add(updated_replica)

                if len(update.modification.modified_metadata) == 1:
                    # Merge modification metadata
                    key = list(update.modification.modified_metadata.keys())[0]
                    assert len(update.modification.modified_metadata[key]) == 1
                    value = update.modification.modified_metadata[key].pop()
                    existing_update.modification.modified_metadata.add(key, value)

        assert len(combined_updates_map) <= len(updates)
        return combined_updates_map.values()

    @staticmethod
    def _data_object_updates_to_generic_update_collection(updates: Sequence[DataObjectUpdate]) -> UpdateCollection:
        """
        Converts the given data object updates to a generic collection of updates that can be stored in the knowledge
        base (`CookieJar`).
        :param updates: the data object updates to convert
        :return: the equivalent generic updates
        """
        generic_update_collection = UpdateCollection()
        for update in updates:
            modification_as_json = BatonUpdateMapper.DATA_OBJECT_MODIFICATION_JSON_ENCODER.default(update.modification)
            metadata = Metadata(modification_as_json)
            update = Update(update.entity.path, update.timestamp, metadata)
            generic_update_collection.append(update)
        return generic_update_collection
