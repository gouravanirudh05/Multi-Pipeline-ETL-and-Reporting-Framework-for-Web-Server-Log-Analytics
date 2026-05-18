import re
import sys
import gzip

# NASA Log pattern from pipelines
LOG_PATTERN = re.compile(r'^(\S+) \S+ \S+ \[(.*?)\] "(\S+) (.*?) (\S+)" (\d{3}) (\S+)')
TS_PATTERN = re.compile(r'^(\d{2})/([A-Za-z]{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2})(?:\s+([+-])(\d{2})(\d{2}))?$')

malformed_java = 0
malformed_python = 0

with gzip.open("data/access_log_Aug95.gz", "rt", encoding="latin-1") as f:
    for i, line in enumerate(f):
        line = line.strip()
        if not line: continue
        
        # Java/JS/Hive logic (requires both regexes)
        is_java_valid = False
        m = LOG_PATTERN.search(line) # .find()
        if m:
            ts = m.group(2)
            if TS_PATTERN.search(ts):
                try:
                    status = int(m.group(6))
                    b = m.group(7)
                    if b == "-" or b.isdigit():
                        is_java_valid = True
                except:
                    pass
        if not is_java_valid:
            malformed_java += 1
            
        # Python (Pig) logic
        is_py_valid = False
        m2 = LOG_PATTERN.match(line)
        if m2:
            ts = m2.group(2)
            try:
                parts = ts.split(' ')
                d_p = parts[0]
                d, mo, r = d_p.split('/')
                tp = r.split(':')
                y, h, mi, s = tp[0], tp[1], tp[2], tp[3]
                status = int(m2.group(6))
                b = m2.group(7)
                if b == "-" or b.isdigit():
                    is_py_valid = True
            except:
                pass
        if not is_py_valid:
            malformed_python += 1
            
print(f"Malformed Java/JS/Hive: {malformed_java}")
print(f"Malformed Python (Pig): {malformed_python}")
