SET hive.cli.print.header=false;
SET hive.exec.dynamic.partition.mode=nonstrict;

DROP DATABASE IF EXISTS ${hivevar:DATABASE} CASCADE;
CREATE DATABASE ${hivevar:DATABASE};
USE ${hivevar:DATABASE};

CREATE EXTERNAL TABLE raw_logs(line STRING)
STORED AS TEXTFILE
LOCATION '${hivevar:INPUT_DIR}';

CREATE TABLE raw_indexed AS
SELECT
  row_number() OVER (ORDER BY line) AS record_number,
  line
FROM raw_logs;

CREATE TABLE extracted AS
SELECT
  record_number,
  line,
  regexp_extract(line, '^(\\S+) \\S+ \\S+ \\[(.*?)\\] "(\\S+) (.*?) (\\S+)" (\\d{3}) (\\S+)', 1) AS host,
  regexp_extract(line, '^(\\S+) \\S+ \\S+ \\[(.*?)\\] "(\\S+) (.*?) (\\S+)" (\\d{3}) (\\S+)', 2) AS timestamp_text,
  regexp_extract(line, '^(\\S+) \\S+ \\S+ \\[(.*?)\\] "(\\S+) (.*?) (\\S+)" (\\d{3}) (\\S+)', 3) AS method,
  regexp_extract(line, '^(\\S+) \\S+ \\S+ \\[(.*?)\\] "(\\S+) (.*?) (\\S+)" (\\d{3}) (\\S+)', 4) AS resource_path,
  regexp_extract(line, '^(\\S+) \\S+ \\S+ \\[(.*?)\\] "(\\S+) (.*?) (\\S+)" (\\d{3}) (\\S+)', 5) AS protocol,
  regexp_extract(line, '^(\\S+) \\S+ \\S+ \\[(.*?)\\] "(\\S+) (.*?) (\\S+)" (\\d{3}) (\\S+)', 6) AS status_text,
  regexp_extract(line, '^(\\S+) \\S+ \\S+ \\[(.*?)\\] "(\\S+) (.*?) (\\S+)" (\\d{3}) (\\S+)', 7) AS bytes_text
FROM raw_indexed;

CREATE TABLE parsed AS
SELECT
  record_number,
  line,
  host,
  timestamp_text,
  method,
  resource_path,
  protocol,
  status_text,
  bytes_text,
  regexp_extract(timestamp_text, '^(\\d{2})/([A-Za-z]{3})/(\\d{4}):(\\d{2}):(\\d{2}):(\\d{2})(?:\\s+([+-])(\\d{2})(\\d{2}))?$', 1) AS day_text,
  regexp_extract(timestamp_text, '^(\\d{2})/([A-Za-z]{3})/(\\d{4}):(\\d{2}):(\\d{2}):(\\d{2})(?:\\s+([+-])(\\d{2})(\\d{2}))?$', 2) AS month_text,
  regexp_extract(timestamp_text, '^(\\d{2})/([A-Za-z]{3})/(\\d{4}):(\\d{2}):(\\d{2}):(\\d{2})(?:\\s+([+-])(\\d{2})(\\d{2}))?$', 3) AS year_text,
  regexp_extract(timestamp_text, '^(\\d{2})/([A-Za-z]{3})/(\\d{4}):(\\d{2}):(\\d{2}):(\\d{2})(?:\\s+([+-])(\\d{2})(\\d{2}))?$', 4) AS hour_text,
  regexp_extract(timestamp_text, '^(\\d{2})/([A-Za-z]{3})/(\\d{4}):(\\d{2}):(\\d{2}):(\\d{2})(?:\\s+([+-])(\\d{2})(\\d{2}))?$', 5) AS minute_text,
  regexp_extract(timestamp_text, '^(\\d{2})/([A-Za-z]{3})/(\\d{4}):(\\d{2}):(\\d{2}):(\\d{2})(?:\\s+([+-])(\\d{2})(\\d{2}))?$', 6) AS second_text
FROM extracted;

CREATE TABLE normalized AS
SELECT
  *,
  CASE month_text
    WHEN 'Jan' THEN '01'
    WHEN 'Feb' THEN '02'
    WHEN 'Mar' THEN '03'
    WHEN 'Apr' THEN '04'
    WHEN 'May' THEN '05'
    WHEN 'Jun' THEN '06'
    WHEN 'Jul' THEN '07'
    WHEN 'Aug' THEN '08'
    WHEN 'Sep' THEN '09'
    WHEN 'Oct' THEN '10'
    WHEN 'Nov' THEN '11'
    WHEN 'Dec' THEN '12'
    ELSE NULL
  END AS month_num
FROM parsed;

CREATE TABLE normalized_validated AS
SELECT
  *,
  CASE WHEN
    host <> ''
    AND method <> ''
    AND resource_path <> ''
    AND protocol <> ''
    AND month_num IS NOT NULL
    AND day_text <> ''
    AND year_text <> ''
    AND hour_text <> ''
    AND minute_text <> ''
    AND second_text <> ''
    AND status_text RLIKE '^\\d{3}$'
    AND (bytes_text = '-' OR bytes_text RLIKE '^\\d+$')
  THEN 1 ELSE 0 END AS is_valid
FROM normalized;

CREATE TABLE valid_epoch AS
SELECT
  min(unix_timestamp(concat(year_text, '-', month_num, '-', day_text, ' ', hour_text, ':', minute_text, ':', second_text), 'yyyy-MM-dd HH:mm:ss')) AS min_epoch,
  min(cast(year_text as int)) AS min_year,
  min(cast(month_num as int)) AS min_month
FROM normalized_validated
WHERE is_valid = 1;

CREATE TABLE annotated_logs AS
SELECT
  n.*,
  CASE
    WHEN '${hivevar:BATCH_MODE}' = 'time' THEN
      CASE
        WHEN is_valid = 1 THEN floor(cast((unix_timestamp(concat(year_text, '-', month_num, '-', day_text, ' ', hour_text, ':', minute_text, ':', second_text), 'yyyy-MM-dd HH:mm:ss') - min_epoch) AS double) / cast(${hivevar:BATCH_VALUE} AS double)) + 1
        ELSE 1
      END
    WHEN '${hivevar:BATCH_MODE}' = 'calendar_month' THEN
      CASE
        WHEN is_valid = 1 THEN ((cast(year_text as int) - min_year) * 12) + (cast(month_num as int) - min_month) + 1
        ELSE 1
      END
    ELSE floor(cast((record_number - 1) AS double) / cast(${hivevar:BATCH_VALUE} AS double)) + 1
  END AS batch_id
FROM normalized_validated n
CROSS JOIN valid_epoch;

CREATE TABLE valid_logs AS
SELECT
  batch_id,
  host,
  concat(year_text, '-', month_num, '-', day_text) AS log_date,
  cast(hour_text AS int) AS log_hour,
  method,
  resource_path,
  protocol,
  cast(status_text AS int) AS status_code,
  CASE WHEN bytes_text = '-' THEN 0 ELSE cast(bytes_text AS bigint) END AS bytes_transferred
FROM annotated_logs
WHERE is_valid = 1;
