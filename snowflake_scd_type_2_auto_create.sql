CREATE OR REPLACE PROCEDURE scd_type_2(source_table STRING, target_table STRING, key_column STRING, hash_column STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var scd_sql = "";

snowflake.execute({sqlText: "BEGIN;"});
try {
    // Step 0: Create target table and add SCD columns if they don't exist.
    snowflake.execute({sqlText: `CREATE TABLE IF NOT EXISTS ${target_table} LIKE ${source_table};`});
    snowflake.execute({sqlText: `ALTER TABLE IF EXISTS ${target_table} ADD COLUMN IF NOT EXISTS START_DATE TIMESTAMP;`});
    snowflake.execute({sqlText: `ALTER TABLE IF EXISTS ${target_table} ADD COLUMN IF NOT EXISTS END_DATE TIMESTAMP;`});
    snowflake.execute({sqlText: `ALTER TABLE IF EXISTS ${target_table} ADD COLUMN IF NOT EXISTS IS_CURRENT BOOLEAN;`});

    // Step 1: Expire records that have changed.
    scd_sql = `
        MERGE INTO ${target_table} AS t
        USING ${source_table} AS s
        ON t.${key_column} = s.${key_column}
        WHEN MATCHED AND t.is_current = TRUE AND t.${hash_column} <> s.${hash_column} THEN
            UPDATE SET t.end_date = CURRENT_TIMESTAMP(), t.is_current = FALSE;
    `;
    snowflake.execute({sqlText: scd_sql});

    // Get the column list from the source table to make the insert dynamic
    var get_cols_stmt = snowflake.createStatement({sqlText: `DESC TABLE ${source_table};`});
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
        INSERT INTO ${target_table} (${columns_str}, start_date, end_date, is_current)
        SELECT ${s_columns_str}, CURRENT_TIMESTAMP(), NULL, TRUE
        FROM ${source_table} AS s
        LEFT JOIN ${target_table} AS t
        ON s.${key_column} = t.${key_column} AND t.is_current = TRUE
        WHERE t.${key_column} IS NULL OR t.${hash_column} <> s.${hash_column};
    `;
    snowflake.execute({sqlText: scd_sql});

    snowflake.execute({sqlText: "COMMIT;"});
    return "SCD Type 2 procedure completed successfully.";

} catch (err) {
    snowflake.execute({sqlText: "ROLLBACK;"});
    throw err;
}
$$;
