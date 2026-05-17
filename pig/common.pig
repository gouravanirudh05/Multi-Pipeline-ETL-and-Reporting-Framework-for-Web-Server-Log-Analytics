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
