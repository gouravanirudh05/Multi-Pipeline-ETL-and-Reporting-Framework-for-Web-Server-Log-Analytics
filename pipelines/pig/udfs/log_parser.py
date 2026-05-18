# -*- coding: utf-8 -*-

import calendar
import re

# ------------------------------------------------------------------ #
# Compiled regex -- same pattern as the Node.js MongoDB pipeline.
# Groups:
#   1  host
#   2  timestamp string  e.g. "01/Jul/1995:00:00:01 -0400"
#   3  HTTP method       e.g. "GET"
#   4  resource path     e.g. "/images/NASA-logosmall.gif"
#   5  protocol          e.g. "HTTP/1.0"
#   6  status code       e.g. "200"
#   7  bytes             e.g. "786" or "-"
# ------------------------------------------------------------------ #
LOG_PATTERN = re.compile(
    r'^(\S+) \S+ \S+ \[(.*?)\] "(\S+) (.*?) (\S+)" (\d{3}) (\S+)'
)
TS_PATTERN = re.compile(
    r'^(\d{2})/([A-Za-z]{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2})(?:\s+([+-])(\d{2})(\d{2}))?$'
)

MONTH_MAP = {
    'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
    'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
    'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
}


@outputSchema('parsed:tuple(host:chararray, timestamp_epoch:long, log_date:chararray, log_hour:int, '
              'method:chararray, resource_path:chararray, '
              'protocol:chararray, status_code:int, bytes_transferred:long)')
def parse_log_line(line):
    """
    Parse one raw NASA HTTP log line into a structured tuple.

    Returns a tuple on success, or None on any parse failure.
    Pig treats a None return as a null row, which we then FILTER out
    and count separately in the orchestrator.

    Pig outputSchema decorator tells Pig Latin the field names and
    types of the returned tuple so it can construct a typed relation.
    """
    if line is None:
        return None

    line = line.strip()
    if not line:
        return None

    match = LOG_PATTERN.match(line)
    if not match:
        return None

    try:
        host        = match.group(1)
        timestamp   = match.group(2)   # "01/Jul/1995:00:00:01 -0400"
        method      = match.group(3)
        resource    = match.group(4)
        protocol    = match.group(5)
        status_code = int(match.group(6))
        raw_bytes   = match.group(7)

        if not host or not method or not resource or not protocol:
            return None

        # Strictly validate timestamp
        if not TS_PATTERN.match(timestamp):
            return None

        # Parse date portion: "01/Jul/1995:00:00:01"
        parts = timestamp.split(' ')
        date_part   = parts[0]          # "01/Jul/1995:00:00:01"
        day, month, rest = date_part.split('/')        # "01", "Jul", "1995:00:00:01"
        time_parts = rest.split(':')
        year, hour = time_parts[0], time_parts[1]
        minute, second = time_parts[2], time_parts[3]

        if month not in MONTH_MAP:
            return None

        log_date = '{}-{}-{}'.format(year, MONTH_MAP[month], day)
        log_hour = int(hour)
        timestamp_epoch = calendar.timegm((
            int(year), int(MONTH_MAP[month]), int(day),
            int(hour), int(minute), int(second), 0, 0, 0
        ))
        if len(parts) > 1 and re.match(r'^[+-]\d{4}$', parts[1]):
            sign = parts[1][0]
            offset_minutes = int(parts[1][1:3]) * 60 + int(parts[1][3:5])
            timestamp_epoch += offset_minutes * 60 if sign == '-' else -offset_minutes * 60

        bytes_transferred = 0 if raw_bytes == '-' else int(raw_bytes)

        return (host, long(timestamp_epoch), log_date, log_hour, method, resource,
                protocol, status_code, bytes_transferred)

    except Exception:
        # Any unexpected structure (missing fields, bad int cast, etc.)
        return None

@outputSchema('ts_ext:tuple(timestamp_epoch:long, log_year:int, log_month:int)')
def extract_timestamp(line):
    if not line:
        return None
    match = re.search(r'\[(\d{2})/([A-Za-z]{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2})', line)
    if not match:
        return None
    try:
        day, month_str, year, hour, minute, second = match.groups()
        if month_str not in MONTH_MAP:
            return None
        epoch = calendar.timegm((
            int(year), int(MONTH_MAP[month_str]), int(day),
            int(hour), int(minute), int(second), 0, 0, 0
        ))
        return (long(epoch), int(year), int(MONTH_MAP[month_str]))
    except Exception:
        return None
