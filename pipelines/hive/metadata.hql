USE ${hivevar:DATABASE};

INSERT OVERWRITE DIRECTORY '${hivevar:OUTPUT_DIR}/batch_metadata'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
  batch_id,
  ${hivevar:BATCH_VALUE} AS batch_size,
  count(1) AS records_processed,
  sum(CASE WHEN is_valid = 1 THEN 0 ELSE 1 END) AS malformed_count
FROM annotated_logs
GROUP BY batch_id;

INSERT OVERWRITE DIRECTORY '${hivevar:OUTPUT_DIR}/malformed_summary'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT batch_id, count(1)
FROM annotated_logs
WHERE is_valid = 0
GROUP BY batch_id;

INSERT OVERWRITE DIRECTORY '${hivevar:OUTPUT_DIR}/malformed_records'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT batch_id, line, 'parse_failed'
FROM annotated_logs
WHERE is_valid = 0;
