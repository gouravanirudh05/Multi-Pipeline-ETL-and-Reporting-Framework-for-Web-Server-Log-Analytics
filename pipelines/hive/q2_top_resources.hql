USE ${hivevar:DATABASE};

INSERT OVERWRITE DIRECTORY '${hivevar:OUTPUT_DIR}/q2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT batch_id, resource_path, request_count, total_bytes, distinct_host_count
FROM (
  SELECT 0 AS batch_id, resource_path, count(1) AS request_count, sum(bytes_transferred) AS total_bytes, count(DISTINCT host) AS distinct_host_count
  FROM valid_logs
  GROUP BY resource_path
  ORDER BY request_count DESC, resource_path ASC
  LIMIT 20
) aggregate_rows
UNION ALL
SELECT batch_id, resource_path, count(1) AS request_count, sum(bytes_transferred) AS total_bytes, count(DISTINCT host) AS distinct_host_count
FROM valid_logs
GROUP BY batch_id, resource_path;
