USE ${hivevar:DATABASE};

INSERT OVERWRITE DIRECTORY '${hivevar:OUTPUT_DIR}/q2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT resource_path, count(1) AS request_count, sum(bytes_transferred) AS total_bytes, count(DISTINCT host) AS distinct_host_count
FROM valid_logs
GROUP BY resource_path
ORDER BY request_count DESC, resource_path ASC
LIMIT 20;
