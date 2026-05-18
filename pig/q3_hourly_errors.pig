hourly_base = FOREACH valid GENERATE
    batch_id AS batch_id,
    log_date AS log_date,
    log_hour AS log_hour,
    host AS host,
    ((status_code >= 400) AND (status_code <= 599) ? 1 : 0) AS is_error:int;

hourly_grouped = GROUP hourly_base BY (batch_id, log_date, log_hour);
hourly_metrics = FOREACH hourly_grouped GENERATE
    group.batch_id AS batch_id,
    group.log_date AS log_date,
    group.log_hour AS log_hour,
    SUM(hourly_base.is_error) AS error_request_count,
    COUNT(hourly_base) AS total_request_count,
    ((double)SUM(hourly_base.is_error) / (double)COUNT(hourly_base)) AS error_rate;

error_only = FILTER hourly_base BY is_error == 1;
error_hosts = FOREACH error_only GENERATE batch_id, log_date, log_hour, host;
distinct_error_hosts = DISTINCT error_hosts;
error_host_grouped = GROUP distinct_error_hosts BY (batch_id, log_date, log_hour);
error_host_counts = FOREACH error_host_grouped GENERATE
    group.batch_id AS batch_id,
    group.log_date AS log_date,
    group.log_hour AS log_hour,
    COUNT(distinct_error_hosts) AS distinct_error_hosts;

q3_joined = JOIN hourly_metrics BY (batch_id, log_date, log_hour) LEFT OUTER, error_host_counts BY (batch_id, log_date, log_hour);
q3 = FOREACH q3_joined GENERATE
    hourly_metrics::batch_id AS batch_id,
    hourly_metrics::log_date AS log_date,
    hourly_metrics::log_hour AS log_hour,
    hourly_metrics::error_request_count AS error_request_count,
    hourly_metrics::total_request_count AS total_request_count,
    hourly_metrics::error_rate AS error_rate,
    (error_host_counts::distinct_error_hosts IS NULL ? 0L : error_host_counts::distinct_error_hosts) AS distinct_error_hosts;

aggregate_hourly_grouped = GROUP hourly_base BY (log_date, log_hour);
aggregate_hourly_metrics = FOREACH aggregate_hourly_grouped GENERATE
    0 AS batch_id,
    group.log_date AS log_date,
    group.log_hour AS log_hour,
    SUM(hourly_base.is_error) AS error_request_count,
    COUNT(hourly_base) AS total_request_count,
    ((double)SUM(hourly_base.is_error) / (double)COUNT(hourly_base)) AS error_rate;
aggregate_error_hosts = FOREACH error_only GENERATE log_date, log_hour, host;
aggregate_distinct_error_hosts = DISTINCT aggregate_error_hosts;
aggregate_error_host_grouped = GROUP aggregate_distinct_error_hosts BY (log_date, log_hour);
aggregate_error_host_counts = FOREACH aggregate_error_host_grouped GENERATE
    0 AS batch_id,
    group.log_date AS log_date,
    group.log_hour AS log_hour,
    COUNT(aggregate_distinct_error_hosts) AS distinct_error_hosts;
aggregate_q3_joined = JOIN aggregate_hourly_metrics BY (log_date, log_hour) LEFT OUTER, aggregate_error_host_counts BY (log_date, log_hour);
aggregate_q3 = FOREACH aggregate_q3_joined GENERATE
    aggregate_hourly_metrics::batch_id AS batch_id,
    aggregate_hourly_metrics::log_date AS log_date,
    aggregate_hourly_metrics::log_hour AS log_hour,
    aggregate_hourly_metrics::error_request_count AS error_request_count,
    aggregate_hourly_metrics::total_request_count AS total_request_count,
    aggregate_hourly_metrics::error_rate AS error_rate,
    (aggregate_error_host_counts::distinct_error_hosts IS NULL ? 0L : aggregate_error_host_counts::distinct_error_hosts) AS distinct_error_hosts;
q3_all = UNION aggregate_q3, q3;

STORE q3_all INTO '$OUTPUT/q3' USING PigStorage('\t');
