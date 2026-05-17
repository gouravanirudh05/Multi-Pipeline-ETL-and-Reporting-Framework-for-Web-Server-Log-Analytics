REGISTER '$UDF_PATH' USING jython AS logudfs;

raw_lines = LOAD '$INPUT' USING TextLoader() AS (line:chararray);
ranked_raw = RANK raw_lines;
ranked_lines = FOREACH ranked_raw GENERATE
    (long)$0 AS record_number,
    (chararray)$1 AS raw_line;

parsed_base = FOREACH ranked_lines GENERATE
    record_number,
    raw_line,
    logudfs.parse_log_line(raw_line) AS parsed:(
        host:chararray,
        timestamp_epoch:long,
        log_date:chararray,
        log_hour:int,
        method:chararray,
        resource_path:chararray,
        protocol:chararray,
        status_code:int,
        bytes_transferred:long
    );

valid_for_min = FILTER parsed_base BY parsed IS NOT NULL;
valid_epochs = FOREACH valid_for_min GENERATE parsed.timestamp_epoch AS timestamp_epoch;
epoch_group = GROUP valid_epochs ALL;
min_epoch_rel = FOREACH epoch_group GENERATE MIN(valid_epochs.timestamp_epoch) AS min_epoch;
parsed_crossed = CROSS parsed_base, min_epoch_rel;

parsed_lines = FOREACH parsed_crossed GENERATE
    parsed_base::record_number AS record_number,
    ($BATCH_BY_TIME == 1 ?
        (parsed_base::parsed IS NULL ? 1 :
            ((int)FLOOR(((double)(parsed_base::parsed.timestamp_epoch - min_epoch_rel::min_epoch)) / ((double)$BATCH_VALUE)) + 1))
        :
        ((int)FLOOR(((double)(parsed_base::record_number - 1L)) / ((double)$BATCH_VALUE)) + 1)
    ) AS batch_id:int,
    parsed_base::raw_line AS raw_line,
    parsed_base::parsed AS parsed;

valid_wrapped = FILTER parsed_lines BY parsed IS NOT NULL;
malformed = FILTER parsed_lines BY parsed IS NULL;

valid = FOREACH valid_wrapped GENERATE
    batch_id AS batch_id,
    FLATTEN(parsed) AS (
        host:chararray,
        timestamp_epoch:long,
        log_date:chararray,
        log_hour:int,
        method:chararray,
        resource_path:chararray,
        protocol:chararray,
        status_code:int,
        bytes_transferred:long
    );

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

q1_grouped = GROUP valid BY (log_date, status_code);
q1 = FOREACH q1_grouped GENERATE
    group.log_date AS log_date,
    group.status_code AS status_code,
    COUNT(valid) AS request_count,
    SUM(valid.bytes_transferred) AS total_bytes;

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

hourly_base = FOREACH valid GENERATE
    log_date AS log_date,
    log_hour AS log_hour,
    host AS host,
    ((status_code >= 400) AND (status_code <= 599) ? 1 : 0) AS is_error:int;

hourly_grouped = GROUP hourly_base BY (log_date, log_hour);
hourly_metrics = FOREACH hourly_grouped GENERATE
    group.log_date AS log_date,
    group.log_hour AS log_hour,
    SUM(hourly_base.is_error) AS error_request_count,
    COUNT(hourly_base) AS total_request_count,
    ((double)SUM(hourly_base.is_error) / (double)COUNT(hourly_base)) AS error_rate;

error_only = FILTER hourly_base BY is_error == 1;
error_hosts = FOREACH error_only GENERATE log_date, log_hour, host;
distinct_error_hosts = DISTINCT error_hosts;
error_host_grouped = GROUP distinct_error_hosts BY (log_date, log_hour);
error_host_counts = FOREACH error_host_grouped GENERATE
    group.log_date AS log_date,
    group.log_hour AS log_hour,
    COUNT(distinct_error_hosts) AS distinct_error_hosts;

q3_joined = JOIN hourly_metrics BY (log_date, log_hour) LEFT OUTER, error_host_counts BY (log_date, log_hour);
q3 = FOREACH q3_joined GENERATE
    hourly_metrics::log_date AS log_date,
    hourly_metrics::log_hour AS log_hour,
    hourly_metrics::error_request_count AS error_request_count,
    hourly_metrics::total_request_count AS total_request_count,
    hourly_metrics::error_rate AS error_rate,
    (error_host_counts::distinct_error_hosts IS NULL ? 0L : error_host_counts::distinct_error_hosts) AS distinct_error_hosts;

STORE q1 INTO '$OUTPUT/q1' USING PigStorage('\t');
STORE q2 INTO '$OUTPUT/q2' USING PigStorage('\t');
STORE q3 INTO '$OUTPUT/q3' USING PigStorage('\t');
STORE batch_metadata INTO '$OUTPUT/batch_metadata' USING PigStorage('\t');
STORE malformed_summary INTO '$OUTPUT/malformed_summary' USING PigStorage('\t');
STORE malformed_records INTO '$OUTPUT/malformed_records' USING PigStorage('\t');
