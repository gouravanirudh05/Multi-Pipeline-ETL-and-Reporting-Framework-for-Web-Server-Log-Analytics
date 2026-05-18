resource_grouped = GROUP valid BY resource_path;
resource_metrics = FOREACH resource_grouped GENERATE
    group AS resource_path,
    COUNT(valid) AS request_count,
    SUM(valid.bytes_transferred) AS total_bytes;

resource_hosts = FOREACH valid GENERATE resource_path, host;
distinct_resource_hosts = DISTINCT resource_hosts;
resource_host_grouped = GROUP distinct_resource_hosts BY resource_path;
resource_host_counts = FOREACH resource_host_grouped GENERATE
    group AS resource_path,
    COUNT(distinct_resource_hosts) AS distinct_host_count;

q2_joined = JOIN resource_metrics BY resource_path, resource_host_counts BY resource_path;
q2_projected = FOREACH q2_joined GENERATE
    resource_metrics::resource_path AS resource_path,
    resource_metrics::request_count AS request_count,
    resource_metrics::total_bytes AS total_bytes,
    resource_host_counts::distinct_host_count AS distinct_host_count;
q2_ordered = ORDER q2_projected BY request_count DESC, resource_path ASC;
q2 = LIMIT q2_ordered 20;

STORE q2 INTO '$OUTPUT/q2' USING PigStorage('\t');
