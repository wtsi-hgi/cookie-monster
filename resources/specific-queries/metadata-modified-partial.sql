SELECT Collection.coll_name AS coll_name,
       Data.data_name AS data_name,
       Data.data_id AS data_id,
       Metadata.meta_id AS meta_id,
       Metadata.meta_attr_name AS meta_attr_name,
       Metadata.meta_attr_value AS meta_attr_value,
       Metadata.meta_attr_unit AS meta_attr_unit,
       MetadataMap.modify_ts AS meta_modify_ts
    FROM R_DATA_MAIN Data
        INNER JOIN R_COLL_MAIN Collection
            ON Data.coll_id = Collection.coll_id
        INNER JOIN R_OBJT_METAMAP MetadataMap
            ON Data.data_id = MetadataMap.object_id
        INNER JOIN R_META_MAIN Metadata
            ON MetadataMap.meta_id = Metadata.meta_id
    WHERE CAST(MetadataMap.modify_ts AS INT) > CAST(? AS INT)
        AND CAST(MetadataMap.modify_ts AS INT) <= CAST(? AS INT)