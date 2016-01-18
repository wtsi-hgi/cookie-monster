SELECT DISTINCT Data.data_id AS data_id
    FROM R_DATA_MAIN Data
        LEFT JOIN R_OBJT_METAMAP MetadataMap
            ON Data.data_id = MetadataMap.object_id
    WHERE CAST(MetadataMap.modify_ts AS INT) > CAST(? AS INT)
        AND CAST(MetadataMap.modify_ts AS INT) <= CAST(? AS INT)