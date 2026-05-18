USE ${hivevar:DATABASE};

INSERT OVERWRITE DIRECTORY '${hivevar:OUTPUT_DIR}/q3'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
  log_date,
  log_hour,
  sum(CASE WHEN status_code >= 400 AND status_code <= 599 THEN 1 ELSE 0 END) AS error_request_count,
  count(1) AS total_request_count,
  cast(sum(CASE WHEN status_code >= 400 AND status_code <= 599 THEN 1 ELSE 0 END) AS double) / cast(count(1) AS double) AS error_rate,
  count(DISTINCT CASE WHEN status_code >= 400 AND status_code <= 599 THEN host ELSE NULL END) AS distinct_error_hosts
FROM valid_logs
GROUP BY log_date, log_hour;
