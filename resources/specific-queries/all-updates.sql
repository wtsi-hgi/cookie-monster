SELECT DISTINCT Collection.coll_name AS collectionName,
                Data.data_name AS dataName,
                Data.data_id AS dataID,
                Data.data_repl_num AS dataReplicationNumber,
                Data.data_checksum AS dataChecksum,
                Data.modify_ts AS dataModifyTimestamp,
                -1 AS metaID,
                '' AS metadataAttrName,
                '' AS metadataAttrValue,
                '' AS metadataAttrUnit,
                NULL AS metadataModifyTimestamp
    FROM R_DATA_MAIN AS Data
        INNER JOIN R_COLL_MAIN AS Collection
            ON Data.coll_id = Collection.coll_id
    WHERE Data.modify_ts >= ? AND Data.modify_ts <= ?
UNION ALL
    SELECT DISTINCT Collection.coll_name AS collectionName,
                Data.data_name AS dataName,
                Data.data_id AS dataID,
                -1 AS dataReplicationNumber,
                '' AS dataChecksum,
                NULL AS dataModifyTimestamp,
                Metadata.meta_id AS metaID,
                Metadata.meta_attr_name AS metadataAttrName,
                Metadata.meta_attr_value AS metadataAttrValue,
                Metadata.meta_attr_unit AS metadataAttrUnit,
                MetadataMap.modify_ts AS metadataModifyTimestamp
    FROM R_DATA_MAIN AS Data
        INNER JOIN R_COLL_MAIN AS Collection
            ON Data.coll_id = Collection.coll_id
        LEFT JOIN R_OBJT_METAMAP AS MetadataMap
            ON Data.data_id = MetadataMap.object_id
        LEFT JOIN R_META_MAIN AS Metadata
            ON MetadataMap.meta_id = Metadata.meta_id
    WHERE MetadataMap.modify_ts >= ? AND MetadataMap.modify_ts <= ?


--SELECT DISTINCT c.coll_name AS collName,
--                d.data_name AS dataName,
--                d.data_id AS dataID,
--                d.data_checksum AS dataChecksum,
--                d.data_repl_num AS dataReplNum,
--                m.meta_id AS metaID,
--                m.meta_attr_name AS metaAttrName,
--                m.meta_attr_value AS metaAttrValue,
--                m.meta_attr_unit AS metaAttrUnit,
--                d.modify_ts AS dataModifyTimestamp,
--                mm.modify_ts AS metaModifyTimestamp
--FROM R_DATA_MAIN d
--JOIN R_COLL_MAIN c ON d.coll_id = c.coll_id
--LEFT JOIN R_OBJT_METAMAP mm ON d.data_id = mm.object_id
--LEFT JOIN R_META_MAIN m ON mm.meta_id = m.meta_id
--WHERE d.modify_ts > ? AND d.modify_ts <= ?
--    OR mm.modify_ts > ? AND mm.modify_ts <= ?