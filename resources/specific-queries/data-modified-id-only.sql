SELECT DISTINCT Data.data_id AS data_id
    FROM R_DATA_MAIN Data
    WHERE CAST(Data.modify_ts AS INT) > CAST(? AS INT)
        AND CAST(Data.modify_ts AS INT) <= CAST(? AS INT)