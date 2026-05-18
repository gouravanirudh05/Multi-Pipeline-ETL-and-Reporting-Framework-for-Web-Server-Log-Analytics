USE ${hivevar:DATABASE};

INSERT OVERWRITE DIRECTORY '${hivevar:OUTPUT_DIR}/q2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT batch_id, resource_path, request_count, total_bytes, distinct_host_count
FROM (
  SELECT
    batch_id,
    resource_path,
    count(1) AS request_count,
    sum(bytes_transferred) AS total_bytes,
    count(DISTINCT host) AS distinct_host_count,
    row_number() OVER (
      PARTITION BY batch_id
      ORDER BY count(1) DESC, resource_path ASC
    ) AS rank_in_batch
  FROM valid_logs
  GROUP BY batch_id, resource_path
) ranked
WHERE rank_in_batch <= 20;
