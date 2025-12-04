CREATE OR REPLACE PROCEDURE SCD_TYPE_2_FINAL(source_table STRING, target_table STRING, key_column STRING, hash_column STRING, log_table STRING)
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
var p_log_table = LOG_TABLE.toUpperCase();

var rows_updated = 0;
var rows_inserted = 0;

try {
    // Step 0: DDL - Ensure target and log tables exist with the correct schema.
    // These are auto-committing and run outside the main procedure transaction.
    snowflake.execute({sqlText: `CREATE TABLE IF NOT EXISTS ${p_target_table} LIKE ${p_source_table};`});
    snowflake.execute({sqlText: `ALTER TABLE IF EXISTS ${p_target_table} ADD COLUMN IF NOT EXISTS START_DATE TIMESTAMP;`});
    snowflake.execute({sqlText: `ALTER TABLE IF EXISTS ${p_target_table} ADD COLUMN IF NOT EXISTS END_DATE TIMESTAMP;`});
    snowflake.execute({sqlText: `ALTER TABLE IF EXISTS ${p_target_table} ADD COLUMN IF NOT EXISTS IS_CURRENT BOOLEAN;`});

    snowflake.execute({sqlText: `
        CREATE TABLE IF NOT EXISTS ${p_log_table} (
            RUN_TIMESTAMP TIMESTAMP_NTZ,
            SOURCE_TABLE VARCHAR,
            TARGET_TABLE VARCHAR,
            STATUS VARCHAR,
            ROWS_UPDATED NUMBER,
            ROWS_INSERTED NUMBER,
            ERROR_MESSAGE VARCHAR
        );
    `});

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

    // Step 1: DML - Execute within the procedure's implicit transaction.
    var merge_sql = `
        MERGE INTO ${p_target_table} AS t
        USING ${p_source_table} AS s
        ON t.${p_key_column} = s.${p_key_column}
        WHEN MATCHED AND t.is_current = TRUE AND t.${p_hash_column} <> s.${p_hash_column} THEN
            UPDATE SET t.end_date = CURRENT_TIMESTAMP(), t.is_current = FALSE;
    `;
    var merge_stmt = snowflake.createStatement({sqlText: merge_sql});
    var merge_rs = merge_stmt.execute();
    merge_rs.next();
    rows_updated = merge_rs.getColumnValue(1);

    var insert_sql = `
        INSERT INTO ${p_target_table} (${columns_str}, start_date, end_date, is_current)
        SELECT ${s_columns_str}, CURRENT_TIMESTAMP(), NULL, TRUE
        FROM ${p_source_table} AS s
        LEFT JOIN ${p_target_table} AS t
        ON s.${p_key_column} = t.${p_key_column} AND t.is_current = TRUE
        WHERE t.${p_key_column} IS NULL OR t.${p_hash_column} <> s.${p_hash_column};
    `;
    var insert_stmt = snowflake.createStatement({sqlText: insert_sql});
    var insert_rs = insert_stmt.execute();
    insert_rs.next();
    rows_inserted = insert_rs.getColumnValue(1);

    // If successful, the implicit transaction will commit upon completion.

    // Log success
    var success_log_sql = `
        INSERT INTO ${p_log_table} (RUN_TIMESTAMP, SOURCE_TABLE, TARGET_TABLE, STATUS, ROWS_UPDATED, ROWS_INSERTED, ERROR_MESSAGE)
        VALUES (CURRENT_TIMESTAMP(), '${p_source_table}', '${p_target_table}', 'SUCCESS', ${rows_updated}, ${rows_inserted}, NULL);
    `;
    snowflake.execute({sqlText: success_log_sql});

    return `SCD Type 2 procedure completed successfully. Rows Updated: ${rows_updated}, Rows Inserted: ${rows_inserted}`;

} catch (err) {
    // An uncaught error will cause Snowflake to automatically roll back the implicit transaction.
    // The INSERT into the log table for failures is removed, as it would be rolled back anyway.
    // The calling application (e.g., Dataiku) is responsible for catching and logging the procedure's failure.
    throw err; // Re-throw the original error to ensure the transaction rolls back and the procedure fails.
}
$$;
