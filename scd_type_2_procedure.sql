CREATE OR REPLACE PROCEDURE scd_type_2(source_table STRING, target_table STRING, key_column STRING, hash_column STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var scd_sql = "";

snowflake.execute({sqlText: "BEGIN;"});
try {
    // Step 1: Expire records that have changed.
    scd_sql = `
        MERGE INTO ${TARGET_TABLE} AS t
        USING ${SOURCE_TABLE} AS s
        ON t.${KEY_COLUMN} = s.${KEY_COLUMN}
        WHEN MATCHED AND t.is_current = TRUE AND t.${HASH_COLUMN} <> s.${HASH_COLUMN} THEN
            UPDATE SET t.end_date = CURRENT_TIMESTAMP(), t.is_current = FALSE;
    `;
    snowflake.execute({sqlText: scd_sql});

    // Get the column list from the source table to make the insert dynamic
    var get_cols_stmt = snowflake.createStatement({sqlText: `DESC TABLE ${SOURCE_TABLE};`});
    var cols_rs = get_cols_stmt.execute();
    var column_list = [];
    while (cols_rs.next()) {
        column_list.push(cols_rs.getColumnValue('name'));
    }

    // Quote column names to handle special characters and preserve case
    var quoted_column_list = column_list.map(c => `"${c}"`);
    var columns_str = quoted_column_list.join(', ');
    var s_columns_str = column_list.map(c => `s."${c}"`).join(', ');

    // Step 2: Insert new records and the new versions of changed records.
    scd_sql = `
        INSERT INTO ${TARGET_TABLE} (${columns_str}, start_date, end_date, is_current)
        SELECT ${s_columns_str}, CURRENT_TIMESTAMP(), NULL, TRUE
        FROM ${SOURCE_TABLE} AS s
        LEFT JOIN ${TARGET_TABLE} AS t
        ON s.${KEY_COLUMN} = t.${KEY_COLUMN} AND t.is_current = TRUE
        WHERE t.${KEY_COLUMN} IS NULL OR t.${HASH_COLUMN} <> s.${HASH_COLUMN};
    `;
    snowflake.execute({sqlText: scd_sql});

    snowflake.execute({sqlText: "COMMIT;"});
    return "SCD Type 2 procedure completed successfully.";

} catch (err) {
    snowflake.execute({sqlText: "ROLLBACK;"});
    throw err;
}
$$;
