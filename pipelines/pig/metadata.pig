batch_rows = FOREACH parsed_lines GENERATE
    batch_id AS batch_id,
    (parsed IS NULL ? 1L : 0L) AS malformed_flag;
batch_grouped = GROUP batch_rows BY batch_id;
batch_metadata = FOREACH batch_grouped GENERATE
    group AS batch_id,
    $BATCH_VALUE AS batch_size,
    COUNT(batch_rows) AS records_processed,
    SUM(batch_rows.malformed_flag) AS malformed_count;

malformed_grouped = GROUP malformed BY batch_id;
malformed_summary = FOREACH malformed_grouped GENERATE
    group AS batch_id,
    COUNT(malformed) AS malformed_count;

malformed_records = FOREACH malformed GENERATE
    batch_id AS batch_id,
    raw_line AS raw_line,
    'parse_failed' AS reason;

STORE batch_metadata INTO '$OUTPUT/batch_metadata' USING PigStorage('\t');
STORE malformed_summary INTO '$OUTPUT/malformed_summary' USING PigStorage('\t');
STORE malformed_records INTO '$OUTPUT/malformed_records' USING PigStorage('\t');
