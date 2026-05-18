resource_grouped = GROUP valid BY (batch_id, resource_path);
resource_metrics = FOREACH resource_grouped GENERATE
    group.batch_id AS batch_id,
    group.resource_path AS resource_path,
    COUNT(valid) AS request_count,
    SUM(valid.bytes_transferred) AS total_bytes;

resource_hosts = FOREACH valid GENERATE batch_id, resource_path, host;
distinct_resource_hosts = DISTINCT resource_hosts;
resource_host_grouped = GROUP distinct_resource_hosts BY (batch_id, resource_path);
resource_host_counts = FOREACH resource_host_grouped GENERATE
    group.batch_id AS batch_id,
    group.resource_path AS resource_path,
    COUNT(distinct_resource_hosts) AS distinct_host_count;

q2_joined = JOIN resource_metrics BY (batch_id, resource_path), resource_host_counts BY (batch_id, resource_path);
q2_projected = FOREACH q2_joined GENERATE
    resource_metrics::batch_id AS batch_id,
    resource_metrics::resource_path AS resource_path,
    resource_metrics::request_count AS request_count,
    resource_metrics::total_bytes AS total_bytes,
    resource_host_counts::distinct_host_count AS distinct_host_count;
q2 = ORDER q2_projected BY batch_id ASC, request_count DESC, resource_path ASC;

aggregate_resource_grouped = GROUP valid BY resource_path;
aggregate_resource_metrics = FOREACH aggregate_resource_grouped GENERATE
    group AS resource_path,
    COUNT(valid) AS request_count,
    SUM(valid.bytes_transferred) AS total_bytes;
aggregate_resource_hosts = FOREACH valid GENERATE resource_path, host;
aggregate_distinct_resource_hosts = DISTINCT aggregate_resource_hosts;
aggregate_resource_host_grouped = GROUP aggregate_distinct_resource_hosts BY resource_path;
aggregate_resource_host_counts = FOREACH aggregate_resource_host_grouped GENERATE
    group AS resource_path,
    COUNT(aggregate_distinct_resource_hosts) AS distinct_host_count;
aggregate_joined = JOIN aggregate_resource_metrics BY resource_path, aggregate_resource_host_counts BY resource_path;
aggregate_projected = FOREACH aggregate_joined GENERATE
    0 AS batch_id,
    aggregate_resource_metrics::resource_path AS resource_path,
    aggregate_resource_metrics::request_count AS request_count,
    aggregate_resource_metrics::total_bytes AS total_bytes,
    aggregate_resource_host_counts::distinct_host_count AS distinct_host_count;
aggregate_ordered = ORDER aggregate_projected BY request_count DESC, resource_path ASC;
aggregate_q2 = LIMIT aggregate_ordered 20;
q2_all = UNION aggregate_q2, q2;

STORE q2_all INTO '$OUTPUT/q2' USING PigStorage('\t');
