SELECT Collection.coll_name AS coll_name,
       Data.data_name AS data_name,
       Data.data_id AS data_id,
       Data.data_repl_num AS data_repl_num,
       Data.data_checksum AS data_checksum,
       Data.modify_ts AS data_modify_ts,
       Data.data_is_dirty AS data_repl_status
    FROM R_DATA_MAIN Data
        INNER JOIN R_COLL_MAIN Collection
            ON Data.coll_id = Collection.coll_id
    WHERE CAST(Data.modify_ts AS INT) > CAST(? AS INT)
        AND CAST(Data.modify_ts AS INT) <= CAST(? AS INT)