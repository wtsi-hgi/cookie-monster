SELECT DISTINCT Collection.coll_name AS collectionName,
                Data.data_name AS dataName,
                Data.data_id AS dataID,
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