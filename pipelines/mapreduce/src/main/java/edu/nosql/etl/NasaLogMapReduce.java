package edu.nosql.etl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NasaLogMapReduce extends Configured implements Tool {
    private static final Pattern LOG_PATTERN = Pattern.compile(
        "^(\\S+) \\S+ \\S+ \\[(.*?)\\] \"(\\S+) (.*?) (\\S+)\" (\\d{3}) (\\S+)"
    );
    private static final Pattern TS_PATTERN = Pattern.compile(
        "^(\\d{2})/([A-Za-z]{3})/(\\d{4}):(\\d{2}):(\\d{2}):(\\d{2})(?:\\s+([+-])(\\d{2})(\\d{2}))?$"
    );
    private static final Map<String, Integer> MONTHS = new HashMap<String, Integer>();

    static {
        MONTHS.put("Jan", 1);
        MONTHS.put("Feb", 2);
        MONTHS.put("Mar", 3);
        MONTHS.put("Apr", 4);
        MONTHS.put("May", 5);
        MONTHS.put("Jun", 6);
        MONTHS.put("Jul", 7);
        MONTHS.put("Aug", 8);
        MONTHS.put("Sep", 9);
        MONTHS.put("Oct", 10);
        MONTHS.put("Nov", 11);
        MONTHS.put("Dec", 12);
    }

    public enum PipelineCounter {
        TOTAL_LINES,
        VALID_LINES,
        MALFORMED_LINES,
        NON_EMPTY_BATCHES
    }

    static final class ParsedLog {
        String host;
        String logDate;
        int logHour;
        String resourcePath;
        int statusCode;
        long bytesTransferred;
        long epochSeconds;
    }

    static ParsedLog parseLogLine(String line) {
        if (line == null || line.trim().isEmpty()) {
            return null;
        }

        Matcher logMatch = LOG_PATTERN.matcher(line.trim());
        if (!logMatch.find()) {
            return null;
        }

        try {
            String timestamp = logMatch.group(2);
            Matcher tsMatch = TS_PATTERN.matcher(timestamp);
            if (!tsMatch.find()) {
                return null;
            }

            int day = Integer.parseInt(tsMatch.group(1));
            String monthToken = tsMatch.group(2);
            Integer month = MONTHS.get(monthToken);
            if (month == null) {
                return null;
            }

            int year = Integer.parseInt(tsMatch.group(3));
            int hour = Integer.parseInt(tsMatch.group(4));
            int minute = Integer.parseInt(tsMatch.group(5));
            int second = Integer.parseInt(tsMatch.group(6));
            ZoneOffset offset = ZoneOffset.UTC;
            if (tsMatch.group(7) != null) {
                offset = ZoneOffset.of(
                    tsMatch.group(7) + tsMatch.group(8) + ":" + tsMatch.group(9)
                );
            }

            ParsedLog parsed = new ParsedLog();
            parsed.host = logMatch.group(1);
            parsed.logDate = String.format(Locale.US, "%04d-%02d-%02d", year, month, day);
            parsed.logHour = hour;
            parsed.resourcePath = logMatch.group(4);
            parsed.statusCode = Integer.parseInt(logMatch.group(6));
            parsed.bytesTransferred = "-".equals(logMatch.group(7))
                ? 0L
                : Long.parseLong(logMatch.group(7));
            parsed.epochSeconds = LocalDateTime
                .of(year, month, day, hour, minute, second)
                .toInstant(offset)
                .getEpochSecond();
            return parsed;
        } catch (RuntimeException ex) {
            return null;
        }
    }

    public static class MinEpochMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static final Text MIN_KEY = new Text("min_epoch");

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            context.getCounter(PipelineCounter.TOTAL_LINES).increment(1);
            ParsedLog parsed = parseLogLine(value.toString());
            if (parsed == null) {
                context.getCounter(PipelineCounter.MALFORMED_LINES).increment(1);
                return;
            }
            context.getCounter(PipelineCounter.VALID_LINES).increment(1);
            context.write(MIN_KEY, new LongWritable(parsed.epochSeconds));
        }
    }

    public static class MinEpochReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long min = Long.MAX_VALUE;
            for (LongWritable value : values) {
                min = Math.min(min, value.get());
            }
            if (min != Long.MAX_VALUE) {
                context.write(key, new LongWritable(min));
            }
        }
    }

    public static class TimeWindowMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
        private long firstEpoch;
        private long intervalSeconds;

        @Override
        protected void setup(Context context) {
            firstEpoch = context.getConfiguration().getLong("nasa.batch.first.epoch", 0L);
            intervalSeconds = context.getConfiguration().getLong("nasa.batch.interval.seconds", 3600L);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            ParsedLog parsed = parseLogLine(value.toString());
            if (parsed == null) {
                return;
            }
            long windowIndex = Math.floorDiv(parsed.epochSeconds - firstEpoch, intervalSeconds);
            context.write(new LongWritable(windowIndex), new LongWritable(1L));
        }
    }

    public static class TimeWindowReducer extends Reducer<LongWritable, LongWritable, Text, LongWritable> {
        private int batchId = 0;

        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long count = 0L;
            for (LongWritable ignored : values) {
                count++;
            }
            batchId++;
            context.getCounter(PipelineCounter.NON_EMPTY_BATCHES).increment(1);
            context.write(new Text(String.valueOf(batchId)), new LongWritable(count));
        }
    }

    public static class DailyTrafficMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            ParsedLog parsed = parseLogLine(value.toString());
            if (parsed == null) {
                return;
            }
            context.write(
                new Text(parsed.logDate + "|" + parsed.statusCode),
                new Text("1\t" + parsed.bytesTransferred)
            );
        }
    }

    public static class DailyTrafficReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            long requestCount = 0L;
            long totalBytes = 0L;
            for (Text value : values) {
                String[] parts = value.toString().split("\\t", -1);
                requestCount += Long.parseLong(parts[0]);
                totalBytes += Long.parseLong(parts[1]);
            }
            context.write(key, new Text(requestCount + "\t" + totalBytes));
        }
    }

    public static class TopResourcesMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            ParsedLog parsed = parseLogLine(value.toString());
            if (parsed == null) {
                return;
            }
            context.write(
                new Text(parsed.resourcePath),
                new Text(parsed.bytesTransferred + "\t" + parsed.host)
            );
        }
    }

    public static class TopResourcesReducer extends Reducer<Text, Text, Text, Text> {
        private PriorityQueue<ResourceMetric> topResources;

        @Override
        protected void setup(Context context) {
            topResources = new PriorityQueue<ResourceMetric>(21, new Comparator<ResourceMetric>() {
                @Override
                public int compare(ResourceMetric left, ResourceMetric right) {
                    int byCount = Long.compare(left.requestCount, right.requestCount);
                    if (byCount != 0) {
                        return byCount;
                    }
                    return right.resourcePath.compareTo(left.resourcePath);
                }
            });
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) {
            long requestCount = 0L;
            long totalBytes = 0L;
            Set<String> hosts = new HashSet<String>();

            for (Text value : values) {
                String[] parts = value.toString().split("\\t", -1);
                requestCount++;
                totalBytes += Long.parseLong(parts[0]);
                hosts.add(parts[1]);
            }

            topResources.offer(
                new ResourceMetric(key.toString(), requestCount, totalBytes, hosts.size())
            );
            if (topResources.size() > 20) {
                topResources.poll();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<ResourceMetric> rows = new ArrayList<ResourceMetric>(topResources);
            Collections.sort(rows, new Comparator<ResourceMetric>() {
                @Override
                public int compare(ResourceMetric left, ResourceMetric right) {
                    int byCount = Long.compare(right.requestCount, left.requestCount);
                    if (byCount != 0) {
                        return byCount;
                    }
                    return left.resourcePath.compareTo(right.resourcePath);
                }
            });

            for (ResourceMetric row : rows) {
                context.write(
                    new Text(row.resourcePath),
                    new Text(row.requestCount + "\t" + row.totalBytes + "\t" + row.distinctHostCount)
                );
            }
        }
    }

    static final class ResourceMetric {
        final String resourcePath;
        final long requestCount;
        final long totalBytes;
        final int distinctHostCount;

        ResourceMetric(String resourcePath, long requestCount, long totalBytes, int distinctHostCount) {
            this.resourcePath = resourcePath;
            this.requestCount = requestCount;
            this.totalBytes = totalBytes;
            this.distinctHostCount = distinctHostCount;
        }
    }

    public static class HourlyErrorsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            ParsedLog parsed = parseLogLine(value.toString());
            if (parsed == null) {
                return;
            }
            boolean isError = parsed.statusCode >= 400 && parsed.statusCode <= 599;
            context.write(
                new Text(parsed.logDate + "|" + parsed.logHour),
                new Text((isError ? "1" : "0") + "\t" + (isError ? parsed.host : ""))
            );
        }
    }

    public static class HourlyErrorsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            long totalRequests = 0L;
            long errorRequests = 0L;
            Set<String> errorHosts = new HashSet<String>();

            for (Text value : values) {
                String[] parts = value.toString().split("\\t", -1);
                totalRequests++;
                if ("1".equals(parts[0])) {
                    errorRequests++;
                    if (parts.length > 1 && !parts[1].isEmpty()) {
                        errorHosts.add(parts[1]);
                    }
                }
            }

            double errorRate = totalRequests == 0
                ? 0.0
                : (double) errorRequests / (double) totalRequests;
            context.write(
                key,
                new Text(errorRequests + "\t" + totalRequests + "\t"
                    + String.format(Locale.US, "%.6f", errorRate) + "\t" + errorHosts.size())
            );
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: NasaLogMapReduce <log_file_path_or_json_array> <batch_mode> <batch_value> <run_uuid>");
            return 2;
        }

        List<String> inputPaths = parsePathList(args[0]);
        String batchMode = args[1];
        int batchValue = Integer.parseInt(args[2]);
        String runUuid = args[3];

        getConf().set("mapreduce.framework.name", "local");
        getConf().set("fs.defaultFS", "file:///");

        if (!"records".equals(batchMode) && !"time".equals(batchMode)) {
            throw new IllegalArgumentException("batch_mode must be records or time");
        }
        if (batchValue <= 0) {
            throw new IllegalArgumentException("batch_value must be greater than 0");
        }

        long startMillis = System.currentTimeMillis();
        Path baseOutput = new Path("/tmp/nasa-log-mapreduce/" + runUuid);
        FileSystem fs = baseOutput.getFileSystem(getConf());
        fs.delete(baseOutput, true);

        System.out.println("MapReduce ETL started");
        System.out.println("Run UUID: " + runUuid);
        System.out.println("Batch mode: " + batchMode);
        System.out.println("Batch value: " + batchValue);
        System.out.println("Input files: " + inputPaths);

        Path metadataOutput = new Path(baseOutput, "metadata");
        Job metadataJob = createJob("nasa-log-metadata", MinEpochMapper.class, MinEpochReducer.class, metadataOutput);
        metadataJob.setMapOutputKeyClass(Text.class);
        metadataJob.setMapOutputValueClass(LongWritable.class);
        metadataJob.setOutputKeyClass(Text.class);
        metadataJob.setOutputValueClass(LongWritable.class);
        addInputs(metadataJob, inputPaths);
        if (!metadataJob.waitForCompletion(true)) {
            throw new IllegalStateException("Metadata MapReduce job failed");
        }

        long totalRecords = metadataJob.getCounters()
            .findCounter(PipelineCounter.TOTAL_LINES)
            .getValue();
        long validRecords = metadataJob.getCounters()
            .findCounter(PipelineCounter.VALID_LINES)
            .getValue();
        long malformedRecords = metadataJob.getCounters()
            .findCounter(PipelineCounter.MALFORMED_LINES)
            .getValue();
        long minEpoch = validRecords > 0 ? readMinEpoch(metadataOutput) : 0L;
        long totalBatches = calculateTotalBatches(
            batchMode,
            batchValue,
            totalRecords,
            validRecords,
            minEpoch,
            inputPaths,
            new Path(baseOutput, "time_windows")
        );

        Path q1Output = new Path(baseOutput, "q1_daily_traffic");
        Job q1Job = createJob("nasa-q1-daily-traffic", DailyTrafficMapper.class, DailyTrafficReducer.class, q1Output);
        q1Job.setMapOutputKeyClass(Text.class);
        q1Job.setMapOutputValueClass(Text.class);
        q1Job.setOutputKeyClass(Text.class);
        q1Job.setOutputValueClass(Text.class);
        addInputs(q1Job, inputPaths);
        if (!q1Job.waitForCompletion(true)) {
            throw new IllegalStateException("Q1 MapReduce job failed");
        }

        Path q2Output = new Path(baseOutput, "q2_top_resources");
        Job q2Job = createJob("nasa-q2-top-resources", TopResourcesMapper.class, TopResourcesReducer.class, q2Output);
        q2Job.setMapOutputKeyClass(Text.class);
        q2Job.setMapOutputValueClass(Text.class);
        q2Job.setOutputKeyClass(Text.class);
        q2Job.setOutputValueClass(Text.class);
        q2Job.setNumReduceTasks(1);
        addInputs(q2Job, inputPaths);
        if (!q2Job.waitForCompletion(true)) {
            throw new IllegalStateException("Q2 MapReduce job failed");
        }

        Path q3Output = new Path(baseOutput, "q3_hourly_errors");
        Job q3Job = createJob("nasa-q3-hourly-errors", HourlyErrorsMapper.class, HourlyErrorsReducer.class, q3Output);
        q3Job.setMapOutputKeyClass(Text.class);
        q3Job.setMapOutputValueClass(Text.class);
        q3Job.setOutputKeyClass(Text.class);
        q3Job.setOutputValueClass(Text.class);
        addInputs(q3Job, inputPaths);
        if (!q3Job.waitForCompletion(true)) {
            throw new IllegalStateException("Q3 MapReduce job failed");
        }

        double runtimeSeconds = (System.currentTimeMillis() - startMillis) / 1000.0;
        double avgBatchSize = totalBatches > 0
            ? (double) totalRecords / (double) totalBatches
            : 0.0;

        loadPostgres(
            runUuid,
            batchMode,
            batchValue,
            totalRecords,
            totalBatches,
            avgBatchSize,
            malformedRecords,
            runtimeSeconds,
            q1Output,
            q2Output,
            q3Output
        );

        System.out.println("MapReduce ETL completed");
        System.out.println("Total Records: " + totalRecords);
        System.out.println("Valid Records: " + validRecords);
        System.out.println("Malformed Records: " + malformedRecords);
        System.out.println("Total Batches: " + totalBatches);
        System.out.println("Avg Batch Size: " + String.format(Locale.US, "%.2f", avgBatchSize));
        System.out.println("Runtime: " + String.format(Locale.US, "%.2f sec", runtimeSeconds));
        return 0;
    }

    private Job createJob(String name, Class<? extends Mapper> mapperClass,
            Class<? extends Reducer> reducerClass, Path outputPath) throws IOException {
        Job job = Job.getInstance(getConf(), name);
        job.setJarByClass(NasaLogMapReduce.class);
        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);
        FileOutputFormat.setOutputPath(job, outputPath);
        return job;
    }

    private void addInputs(Job job, List<String> inputPaths) throws IOException {
        for (String inputPath : inputPaths) {
            FileInputFormat.addInputPath(job, new Path(inputPath));
        }
    }

    private long calculateTotalBatches(String batchMode, int batchValue, long totalRecords,
            long validRecords, long minEpoch, List<String> inputPaths, Path windowOutput)
            throws Exception {
        if ("records".equals(batchMode)) {
            return totalRecords == 0 ? 0 : (totalRecords + batchValue - 1) / batchValue;
        }
        if (validRecords == 0) {
            return totalRecords == 0 ? 0 : 1;
        }

        Job windowJob = createJob(
            "nasa-time-window-batches",
            TimeWindowMapper.class,
            TimeWindowReducer.class,
            windowOutput
        );
        windowJob.getConfiguration().setLong("nasa.batch.first.epoch", minEpoch);
        windowJob.getConfiguration().setLong("nasa.batch.interval.seconds", batchValue);
        windowJob.setMapOutputKeyClass(LongWritable.class);
        windowJob.setMapOutputValueClass(LongWritable.class);
        windowJob.setOutputKeyClass(Text.class);
        windowJob.setOutputValueClass(LongWritable.class);
        windowJob.setNumReduceTasks(1);
        addInputs(windowJob, inputPaths);

        if (!windowJob.waitForCompletion(true)) {
            throw new IllegalStateException("Time-window batch MapReduce job failed");
        }
        return windowJob.getCounters().findCounter(PipelineCounter.NON_EMPTY_BATCHES).getValue();
    }

    private long readMinEpoch(Path metadataOutput) throws IOException {
        for (String line : readPartLines(metadataOutput)) {
            String[] parts = line.split("\\t", -1);
            if (parts.length >= 2 && "min_epoch".equals(parts[0])) {
                return Long.parseLong(parts[1]);
            }
        }
        return 0L;
    }

    private void loadPostgres(String runUuid, String batchMode, int batchValue,
            long totalRecords, long totalBatches, double avgBatchSize, long malformedRecords,
            double runtimeSeconds, Path q1Output, Path q2Output, Path q3Output) throws Exception {
        StringBuilder sql = new StringBuilder();
        sql.append(schemaSql());
        sql.append("INSERT INTO etl_runs ")
            .append("(run_uuid, pipeline, batch_size, batch_mode, batch_interval_seconds, status, started_at) VALUES (")
            .append(sqlString(runUuid)).append(", ")
            .append("'mapreduce', ")
            .append("records".equals(batchMode) ? String.valueOf(batchValue) : "NULL").append(", ")
            .append(sqlString(batchMode)).append(", ")
            .append("time".equals(batchMode) ? String.valueOf(batchValue) : "NULL").append(", ")
            .append("'running', NOW()) ")
            .append("ON CONFLICT (run_uuid) DO UPDATE SET ")
            .append("pipeline = EXCLUDED.pipeline, ")
            .append("batch_size = EXCLUDED.batch_size, ")
            .append("batch_mode = EXCLUDED.batch_mode, ")
            .append("batch_interval_seconds = EXCLUDED.batch_interval_seconds, ")
            .append("status = EXCLUDED.status;\n");
        sql.append("DELETE FROM daily_traffic WHERE run_uuid = ").append(sqlString(runUuid)).append(";\n");
        sql.append("DELETE FROM top_resources WHERE run_uuid = ").append(sqlString(runUuid)).append(";\n");
        sql.append("DELETE FROM hourly_errors WHERE run_uuid = ").append(sqlString(runUuid)).append(";\n");

        appendQ1Inserts(sql, runUuid, totalBatches, q1Output);
        appendQ2Inserts(sql, runUuid, totalBatches, q2Output);
        appendQ3Inserts(sql, runUuid, totalBatches, q3Output);

        sql.append("UPDATE etl_runs SET ")
            .append("total_records = ").append(totalRecords).append(", ")
            .append("total_batches = ").append(totalBatches).append(", ")
            .append("avg_batch_size = ").append(String.format(Locale.US, "%.4f", avgBatchSize)).append(", ")
            .append("malformed_count = ").append(malformedRecords).append(", ")
            .append("runtime_seconds = ").append(String.format(Locale.US, "%.4f", runtimeSeconds)).append(", ")
            .append("status = 'completed', ")
            .append("completed_at = NOW(), ")
            .append("batch_mode = ").append(sqlString(batchMode)).append(", ")
            .append("batch_interval_seconds = ")
            .append("time".equals(batchMode) ? String.valueOf(batchValue) : "NULL")
            .append(" WHERE run_uuid = ").append(sqlString(runUuid)).append(";\n");

        runPsql(sql.toString());
    }

    private void appendQ1Inserts(StringBuilder sql, String runUuid, long batchId, Path outputPath)
            throws IOException {
        for (String line : readPartLines(outputPath)) {
            String[] parts = line.split("\\t", -1);
            if (parts.length < 3) {
                continue;
            }
            String[] keyParts = parts[0].split("\\|", -1);
            if (keyParts.length < 2) {
                continue;
            }
            sql.append("INSERT INTO daily_traffic ")
                .append("(pipeline, run_uuid, batch_id, log_date, status_code, request_count, total_bytes) VALUES (")
                .append("'mapreduce', ")
                .append(sqlString(runUuid)).append(", ")
                .append(batchId).append(", ")
                .append(sqlString(keyParts[0])).append(", ")
                .append(keyParts[1]).append(", ")
                .append(parts[1]).append(", ")
                .append(parts[2]).append(");\n");
        }
    }

    private void appendQ2Inserts(StringBuilder sql, String runUuid, long batchId, Path outputPath)
            throws IOException {
        for (String line : readPartLines(outputPath)) {
            String[] parts = line.split("\\t", -1);
            if (parts.length < 4) {
                continue;
            }
            sql.append("INSERT INTO top_resources ")
                .append("(pipeline, run_uuid, batch_id, resource_path, request_count, total_bytes, distinct_host_count) VALUES (")
                .append("'mapreduce', ")
                .append(sqlString(runUuid)).append(", ")
                .append(batchId).append(", ")
                .append(sqlString(parts[0])).append(", ")
                .append(parts[1]).append(", ")
                .append(parts[2]).append(", ")
                .append(parts[3]).append(");\n");
        }
    }

    private void appendQ3Inserts(StringBuilder sql, String runUuid, long batchId, Path outputPath)
            throws IOException {
        for (String line : readPartLines(outputPath)) {
            String[] parts = line.split("\\t", -1);
            if (parts.length < 5) {
                continue;
            }
            String[] keyParts = parts[0].split("\\|", -1);
            if (keyParts.length < 2) {
                continue;
            }
            sql.append("INSERT INTO hourly_errors ")
                .append("(pipeline, run_uuid, batch_id, log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts) VALUES (")
                .append("'mapreduce', ")
                .append(sqlString(runUuid)).append(", ")
                .append(batchId).append(", ")
                .append(sqlString(keyParts[0])).append(", ")
                .append(keyParts[1]).append(", ")
                .append(parts[1]).append(", ")
                .append(parts[2]).append(", ")
                .append(parts[3]).append(", ")
                .append(parts[4]).append(");\n");
        }
    }

    private List<String> readPartLines(Path outputPath) throws IOException {
        List<String> rows = new ArrayList<String>();
        FileSystem fs = outputPath.getFileSystem(getConf());
        if (!fs.exists(outputPath)) {
            return rows;
        }
        for (FileStatus status : fs.listStatus(outputPath)) {
            String name = status.getPath().getName();
            if (!name.startsWith("part-")) {
                continue;
            }
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(fs.open(status.getPath()), StandardCharsets.UTF_8)
            );
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.trim().isEmpty()) {
                        rows.add(line);
                    }
                }
            } finally {
                reader.close();
            }
        }
        return rows;
    }

    private String schemaSql() {
        return ""
            + "CREATE TABLE IF NOT EXISTS etl_runs ("
            + "run_uuid VARCHAR(64) PRIMARY KEY, "
            + "pipeline VARCHAR(20), "
            + "batch_size INTEGER, "
            + "total_records INTEGER, "
            + "total_batches INTEGER, "
            + "avg_batch_size NUMERIC(10,2), "
            + "malformed_count INTEGER, "
            + "runtime_seconds NUMERIC(10,3), "
            + "status VARCHAR(20), "
            + "started_at TIMESTAMPTZ, "
            + "completed_at TIMESTAMPTZ"
            + ");\n"
            + "ALTER TABLE etl_runs ADD COLUMN IF NOT EXISTS batch_mode VARCHAR(20) DEFAULT 'records';\n"
            + "ALTER TABLE etl_runs ADD COLUMN IF NOT EXISTS batch_interval_seconds INTEGER;\n"
            + "CREATE TABLE IF NOT EXISTS daily_traffic ("
            + "id SERIAL PRIMARY KEY, pipeline VARCHAR(20), run_uuid VARCHAR(64), batch_id INTEGER, "
            + "executed_at TIMESTAMPTZ DEFAULT NOW(), log_date DATE, status_code INTEGER, "
            + "request_count BIGINT, total_bytes BIGINT"
            + ");\n"
            + "CREATE TABLE IF NOT EXISTS top_resources ("
            + "id SERIAL PRIMARY KEY, pipeline VARCHAR(20), run_uuid VARCHAR(64), batch_id INTEGER, "
            + "executed_at TIMESTAMPTZ DEFAULT NOW(), resource_path TEXT, request_count BIGINT, "
            + "total_bytes BIGINT, distinct_host_count BIGINT"
            + ");\n"
            + "CREATE TABLE IF NOT EXISTS hourly_errors ("
            + "id SERIAL PRIMARY KEY, pipeline VARCHAR(20), run_uuid VARCHAR(64), batch_id INTEGER, "
            + "executed_at TIMESTAMPTZ DEFAULT NOW(), log_date DATE, log_hour SMALLINT, "
            + "error_request_count BIGINT, total_request_count BIGINT, error_rate NUMERIC(6,4), "
            + "distinct_error_hosts BIGINT"
            + ");\n";
    }

    private void runPsql(String sql) throws Exception {
        java.nio.file.Path sqlFile = Files.createTempFile("nasa-mapreduce-", ".sql");
        BufferedWriter writer = Files.newBufferedWriter(sqlFile, StandardCharsets.UTF_8);
        try {
            writer.write(sql);
        } finally {
            writer.close();
        }

        String host = envOrDefault("PGHOST", "127.0.0.1");
        String port = envOrDefault("PGPORT", "5432");
        String database = envOrDefault("PGDATABASE", "nosql_etl_db");
        String user = envOrDefault("PGUSER", "sathish");
        String password = envOrDefault("PGPASSWORD", "welcome");

        ProcessBuilder builder = new ProcessBuilder(
            "psql",
            "-h", host,
            "-p", port,
            "-U", user,
            "-d", database,
            "-v", "ON_ERROR_STOP=1",
            "-f", sqlFile.toString()
        );
        builder.redirectErrorStream(true);
        builder.environment().put("PGPASSWORD", password);
        Process process = builder.start();
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8)
        );
        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
        int exitCode = process.waitFor();
        Files.deleteIfExists(sqlFile);
        if (exitCode != 0) {
            throw new IllegalStateException("psql failed with exit code " + exitCode);
        }
    }

    private String envOrDefault(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.trim().isEmpty() ? defaultValue : value;
    }

    private String sqlString(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private static List<String> parsePathList(String raw) {
        String trimmed = raw.trim();
        if (!trimmed.startsWith("[")) {
            return Collections.singletonList(trimmed);
        }

        List<String> paths = new ArrayList<String>();
        StringBuilder current = new StringBuilder();
        boolean inString = false;
        boolean escaping = false;

        for (int i = 0; i < trimmed.length(); i++) {
            char ch = trimmed.charAt(i);
            if (escaping) {
                current.append(ch);
                escaping = false;
                continue;
            }
            if (ch == '\\') {
                escaping = true;
                continue;
            }
            if (ch == '"') {
                if (inString) {
                    paths.add(current.toString());
                    current.setLength(0);
                }
                inString = !inString;
                continue;
            }
            if (inString) {
                current.append(ch);
            }
        }

        return paths.isEmpty() ? Arrays.asList(trimmed) : paths;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new NasaLogMapReduce(), args);
        System.exit(exitCode);
    }
}
