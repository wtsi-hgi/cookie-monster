SELECT DISTINCT Collection.coll_name AS collectionName,
                Data.data_name AS dataName,
                Data.data_id AS dataID,
                Data.data_repl_num AS dataReplicaNumber,
                Data.data_checksum AS dataChecksum,
                Data.modify_ts AS dataModifyTimestamp
    FROM R_DATA_MAIN AS Data
        INNER JOIN R_COLL_MAIN AS Collection
            ON Data.coll_id = Collection.coll_id
    WHERE Data.modify_ts >= ? AND Data.modify_ts <= ?