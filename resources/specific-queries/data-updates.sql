SELECT DISTINCT Collection.coll_name AS collectionName,
                Data.data_name AS dataName,
                Data.data_id AS dataID,
                Data.data_repl_num AS dataReplicaNumber,
                Data.data_checksum AS dataChecksum,
                Data.modify_ts AS dataModifyTimestamp,
                Data.data_is_dirty AS dataReplicaStatus
    FROM R_DATA_MAIN Data
        INNER JOIN R_COLL_MAIN Collection
            ON Data.coll_id = Collection.coll_id
    WHERE CAST(Data.modify_ts AS INT) > CAST(? AS INT)
        AND CAST(Data.modify_ts AS INT) <= CAST(? AS INT)