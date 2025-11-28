CREATE OR REPLACE PROCEDURE SCD_TYPE_2_AUTO_TBL(source_table STRING, target_table STRING, key_column STRING, hash_column STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
// Snowflake procedure arguments are passed in uppercase.
var p_source_table = SOURCE_TABLE.toUpperCase();
var p_target_table = TARGET_TABLE.toUpperCase();
var p_key_column = KEY_COLUMN.toUpperCase();
var p_hash_column = HASH_COLUMN.toUpperCase();

try {
    // Step 0: DDL - Ensure target table exists with the correct schema.
    // These statements are executed immediately, are auto-committing,
    // and guarantee the table exists before the DML is compiled.
    snowflake.execute({sqlText: `CREATE TABLE IF NOT EXISTS ${p_target_table} LIKE ${p_source_table};`});
    snowflake.execute({sqlText: `ALTER TABLE IF EXISTS ${p_target_table} ADD COLUMN IF NOT EXISTS START_DATE TIMESTAMP;`});
    snowflake.execute({sqlText: `ALTER TABLE IF EXISTS ${p_target_table} ADD COLUMN IF NOT EXISTS END_DATE TIMESTAMP;`});
    snowflake.execute({sqlText: `ALTER TABLE IF EXISTS ${p_target_table} ADD COLUMN IF NOT EXISTS IS_CURRENT BOOLEAN;`});

    // Get the column list from the source table to make the insert dynamic.
    var get_cols_stmt = snowflake.createStatement({sqlText: `DESC TABLE ${p_source_table};`});
    var cols_rs = get_cols_stmt.execute();
    var column_list = [];
    while (cols_rs.next()) {
        column_list.push(cols_rs.getColumnValue('name'));
    }

    var quoted_column_list = column_list.map(c => `"${c}"`);
    var columns_str = quoted_column_list.join(', ');
    var s_columns_str = column_list.map(c => `s."${c}"`).join(', ');

    // Step 1: DML - Execute as an atomic transaction.
    snowflake.execute({sqlText: "BEGIN;"});

    var merge_sql = `
        MERGE INTO ${p_target_table} AS t
        USING ${p_source_table} AS s
        ON t.${p_key_column} = s.${p_key_column}
        WHEN MATCHED AND t.is_current = TRUE AND t.${p_hash_column} <> s.${p_hash_column} THEN
            UPDATE SET t.end_date = CURRENT_TIMESTAMP(), t.is_current = FALSE;
    `;
    snowflake.execute({sqlText: merge_sql});

    var insert_sql = `
        INSERT INTO ${p_target_table} (${columns_str}, start_date, end_date, is_current)
        SELECT ${s_columns_str}, CURRENT_TIMESTAMP(), NULL, TRUE
        FROM ${p_source_table} AS s
        LEFT JOIN ${p_target_table} AS t
        ON s.${p_key_column} = t.${p_key_column} AND t.is_current = TRUE
        WHERE t.${p_key_column} IS NULL OR t.${p_hash_column} <> s.${p_hash_column};
    `;
    snowflake.execute({sqlText: insert_sql});

    snowflake.execute({sqlText: "COMMIT;"});

    return "SCD Type 2 procedure completed successfully.";

} catch (err) {
    // If any DML step fails, roll back the transaction.
    snowflake.execute({sqlText: "ROLLBACK;"});
    throw err; // Re-throw the error to fail the procedure call.
}
$$;
