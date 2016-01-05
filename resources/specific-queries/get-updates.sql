SELECT DISTINCT c.coll_name AS collName,
                d.data_name AS dataName,
                d.data_id AS dataID,
                d.data_checksum AS dataChecksum,
                d.data_repl_num AS dataReplNum,
                m.meta_id AS metaID,
                m.meta_attr_name AS metaAttrName,
                m.meta_attr_value AS metaAttrValue,
                m.meta_attr_unit AS metaAttrUnit,
                d.modify_ts AS dataModifyTimestamp,
                mm.modify_ts AS metaModifyTimestamp
FROM R_DATA_MAIN d
JOIN R_COLL_MAIN c ON d.coll_id = c.coll_id
LEFT JOIN R_OBJT_METAMAP mm ON d.data_id = mm.object_id
LEFT JOIN R_META_MAIN m ON mm.meta_id = m.meta_id
WHERE d.modify_ts > ? AND d.modify_ts <= ?
    OR mm.modify_ts > ? AND mm.modify_ts <= ?