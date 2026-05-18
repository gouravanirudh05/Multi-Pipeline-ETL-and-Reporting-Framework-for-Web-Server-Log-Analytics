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
import java.util.TreeMap;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
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

    static final class BatchStat {
        final long batchId;
        long recordsProcessed;
        long malformedCount;
        final List<String> malformedLines = new ArrayList<String>();

        BatchStat(long batchId) {
            this.batchId = batchId;
        }
    }

    static final class BatchAssignedLog {
        long batchId;
        String host;
        String logDate;
        int logHour;
        String resourcePath;
        int statusCode;
        long bytesTransferred;
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

            String host = logMatch.group(1);
            String method = logMatch.group(3);
            String resourcePath = logMatch.group(4);
            if (resourcePath == null || resourcePath.isEmpty()) {
                return null;
            }
            String protocol = logMatch.group(5);

            if (host.isEmpty() || method.isEmpty() || resourcePath.isEmpty() || protocol.isEmpty()) {
                return null;
            }

            ParsedLog parsed = new ParsedLog();
            parsed.host = host;
            parsed.logDate = String.format(Locale.US, "%04d-%02d-%02d", year, month, day);
            parsed.logHour = hour;
            parsed.resourcePath = resourcePath;
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

    static BatchAssignedLog parseBatchAssignedLog(String line) {
        if (line == null || !line.startsWith("V|")) {
            return null;
        }
        String[] parts = line.split("\\t", -1);
        if (parts.length < 7) {
            return null;
        }
        try {
            BatchAssignedLog parsed = new BatchAssignedLog();
            parsed.batchId = Long.parseLong(parts[0].substring(2));
            parsed.host = parts[1];
            parsed.logDate = parts[2];
            parsed.logHour = Integer.parseInt(parts[3]);
            parsed.resourcePath = parts[4];
            parsed.statusCode = Integer.parseInt(parts[5]);
            parsed.bytesTransferred = Long.parseLong(parts[6]);
            return parsed;
        } catch (RuntimeException ex) {
            return null;
        }
    }

    public static class MinEpochMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static final Text MIN_KEY = new Text("min_epoch");
        private static final Text MIN_YEAR = new Text("min_year");
        private static final Text MIN_MONTH = new Text("min_month");

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
            if (parsed.logDate != null && parsed.logDate.length() >= 7) {
                context.write(MIN_YEAR, new LongWritable(Integer.parseInt(parsed.logDate.substring(0, 4))));
                context.write(MIN_MONTH, new LongWritable(Integer.parseInt(parsed.logDate.substring(5, 7))));
            }
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
        private String batchMode;
        private long firstYear;
        private long firstMonth;

        @Override
        protected void setup(Context context) {
            firstEpoch = context.getConfiguration().getLong("nasa.batch.first.epoch", 0L);
            intervalSeconds = context.getConfiguration().getLong("nasa.batch.interval.seconds", 3600L);
            batchMode = context.getConfiguration().get("nasa.batch.mode", "time");
            firstYear = context.getConfiguration().getLong("nasa.batch.first.year", 0L);
            firstMonth = context.getConfiguration().getLong("nasa.batch.first.month", 0L);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            ParsedLog parsed = parseLogLine(value.toString());
            if (parsed == null) {
                return;
            }
            long windowIndex;
            if ("calendar_month".equals(batchMode) && parsed.logDate != null && parsed.logDate.length() >= 7) {
                long year = Integer.parseInt(parsed.logDate.substring(0, 4));
                long month = Integer.parseInt(parsed.logDate.substring(5, 7));
                windowIndex = (year - firstYear) * 12 + (month - firstMonth);
            } else {
                windowIndex = Math.floorDiv(parsed.epochSeconds - firstEpoch, intervalSeconds);
            }
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

    public static class BatchMetadataMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            String sortKey = split.getPath().toString() + "\t"
                + String.format(Locale.US, "%020d", key.get());
            context.write(new Text(sortKey), value);
        }
    }

    public static class BatchMetadataReducer extends Reducer<Text, Text, Text, Text> {
        private String batchMode;
        private long batchValue;
        private long firstEpoch;
        private long firstYear;
        private long firstMonth;
        private long totalSeen = 0L;
        private long activeBatchId = 1L;
        private long nextTimeBatchId = 1L;
        private final Map<Long, Long> timeWindowBatchIds = new HashMap<Long, Long>();
        private final Map<Long, BatchStat> batchStats = new TreeMap<Long, BatchStat>();

        @Override
        protected void setup(Context context) {
            batchMode = context.getConfiguration().get("nasa.batch.mode", "records");
            batchValue = context.getConfiguration().getLong("nasa.batch.value", 10000L);
            firstEpoch = context.getConfiguration().getLong("nasa.batch.first.epoch", 0L);
            firstYear = context.getConfiguration().getLong("nasa.batch.first.year", 0L);
            firstMonth = context.getConfiguration().getLong("nasa.batch.first.month", 0L);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                String line = value.toString();
                totalSeen++;

                ParsedLog parsed = parseLogLine(line);
                long batchId;
                if ("records".equals(batchMode)) {
                    batchId = ((totalSeen - 1L) / batchValue) + 1L;
                } else if (parsed != null) {
                    long window;
                    if ("calendar_month".equals(batchMode) && parsed.logDate != null && parsed.logDate.length() >= 7) {
                        long year = Integer.parseInt(parsed.logDate.substring(0, 4));
                        long month = Integer.parseInt(parsed.logDate.substring(5, 7));
                        window = (year - firstYear) * 12 + (month - firstMonth);
                    } else {
                        window = Math.floorDiv(parsed.epochSeconds - firstEpoch, batchValue);
                    }
                    Long existingBatchId = timeWindowBatchIds.get(window);
                    if (existingBatchId == null) {
                        existingBatchId = nextTimeBatchId++;
                        timeWindowBatchIds.put(window, existingBatchId);
                    }
                    batchId = existingBatchId.longValue();
                    activeBatchId = batchId;
                } else {
                    batchId = activeBatchId;
                }

                BatchStat stat = batchStats.get(batchId);
                if (stat == null) {
                    stat = new BatchStat(batchId);
                    batchStats.put(batchId, stat);
                }
                stat.recordsProcessed++;

                if (parsed == null) {
                    stat.malformedCount++;
                    context.write(new Text("M|" + batchId), new Text(line + "\tparse_failed"));
                } else {
                    context.write(
                        new Text("V|" + batchId),
                        new Text(
                            parsed.host + "\t"
                                + parsed.logDate + "\t"
                                + parsed.logHour + "\t"
                                + parsed.resourcePath + "\t"
                                + parsed.statusCode + "\t"
                                + parsed.bytesTransferred
                        )
                    );
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (BatchStat stat : batchStats.values()) {
                context.write(
                    new Text("B|" + stat.batchId),
                    new Text(batchValue + "\t" + stat.recordsProcessed + "\t" + stat.malformedCount)
                );
            }
        }
    }

    public static class DailyTrafficMapper extends Mapper<LongWritable, Text, Text, Text> {
        private String aggregationMode;
        private long resultBatchId;

        @Override
        protected void setup(Context context) {
            aggregationMode = context.getConfiguration().get("nasa.aggregation.mode", "global");
            resultBatchId = context.getConfiguration().getLong("nasa.result.batch.id", 0L);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            BatchAssignedLog parsed = parseBatchAssignedLog(value.toString());
            if (parsed == null) {
                return;
            }
            long batchId = "per_batch".equals(aggregationMode) ? parsed.batchId : resultBatchId;
            context.write(
                new Text(batchId + "|" + parsed.logDate + "|" + parsed.statusCode),
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
        private String aggregationMode;
        private long resultBatchId;

        @Override
        protected void setup(Context context) {
            aggregationMode = context.getConfiguration().get("nasa.aggregation.mode", "global");
            resultBatchId = context.getConfiguration().getLong("nasa.result.batch.id", 0L);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            BatchAssignedLog parsed = parseBatchAssignedLog(value.toString());
            if (parsed == null) {
                return;
            }
            long batchId = "per_batch".equals(aggregationMode) ? parsed.batchId : resultBatchId;
            context.write(
                new Text(batchId + "|" + parsed.resourcePath),
                new Text(parsed.bytesTransferred + "\t" + parsed.host)
            );
        }
    }

    public static class TopResourcesReducer extends Reducer<Text, Text, Text, Text> {
        private Map<Long, PriorityQueue<ResourceMetric>> topResourcesByBatch;

        @Override
        protected void setup(Context context) {
            topResourcesByBatch = new TreeMap<Long, PriorityQueue<ResourceMetric>>();
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

            ResourceMetric row = new ResourceMetric(key.toString(), requestCount, totalBytes, hosts.size());
            PriorityQueue<ResourceMetric> topResources = topResourcesByBatch.get(row.batchId);
            if (topResources == null) {
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
                topResourcesByBatch.put(row.batchId, topResources);
            }
            topResources.offer(row);
            if (topResources.size() > 20) {
                topResources.poll();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Long, PriorityQueue<ResourceMetric>> entry : topResourcesByBatch.entrySet()) {
                List<ResourceMetric> rows = new ArrayList<ResourceMetric>(entry.getValue());
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
                        new Text(row.batchId + "|" + row.resourcePath),
                        new Text(row.requestCount + "\t" + row.totalBytes + "\t" + row.distinctHostCount)
                    );
                }
            }
        }
    }

    static final class ResourceMetric {
        final long batchId;
        final String resourcePath;
        final long requestCount;
        final long totalBytes;
        final int distinctHostCount;

        ResourceMetric(String compositeKey, long requestCount, long totalBytes, int distinctHostCount) {
            String[] keyParts = compositeKey.split("\\|", 2);
            this.batchId = Long.parseLong(keyParts[0]);
            this.resourcePath = keyParts.length > 1 ? keyParts[1] : compositeKey;
            this.requestCount = requestCount;
            this.totalBytes = totalBytes;
            this.distinctHostCount = distinctHostCount;
        }
    }

    public static class HourlyErrorsMapper extends Mapper<LongWritable, Text, Text, Text> {
        private String aggregationMode;
        private long resultBatchId;

        @Override
        protected void setup(Context context) {
            aggregationMode = context.getConfiguration().get("nasa.aggregation.mode", "global");
            resultBatchId = context.getConfiguration().getLong("nasa.result.batch.id", 0L);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            BatchAssignedLog parsed = parseBatchAssignedLog(value.toString());
            if (parsed == null) {
                return;
            }
            boolean isError = parsed.statusCode >= 400 && parsed.statusCode <= 599;
            long batchId = "per_batch".equals(aggregationMode) ? parsed.batchId : resultBatchId;
            context.write(
                new Text(batchId + "|" + parsed.logDate + "|" + parsed.logHour),
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
        int batchValue = "calendar_month".equals(batchMode) ? 0 : Integer.parseInt(args[2]);
        String runUuid = args[3];
        String query = args.length >= 5 ? args[4] : "all";
        String aggregationMode = args.length >= 6 ? args[5] : "global";

        getConf().set("mapreduce.framework.name", "local");

        if (!"records".equals(batchMode) && !"time".equals(batchMode) && !"calendar_month".equals(batchMode)) {
            throw new IllegalArgumentException("batch_mode must be records, time, or calendar_month");
        }
        if (!"calendar_month".equals(batchMode) && batchValue <= 0) {
            throw new IllegalArgumentException("batch_value must be greater than 0");
        }
        if (!"all".equals(query) && !"q1".equals(query) && !"q2".equals(query) && !"q3".equals(query)) {
            throw new IllegalArgumentException("query must be one of all, q1, q2, q3");
        }
        if (!"global".equals(aggregationMode) && !"per_batch".equals(aggregationMode)) {
            throw new IllegalArgumentException("aggregation_mode must be one of: global, per_batch");
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
        metadataJob.setCombinerClass(MinEpochReducer.class);
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
        long firstEpoch = readMinEpoch(metadataOutput, "min_epoch");
        long firstYear = readMinEpoch(metadataOutput, "min_year");
        long firstMonth = readMinEpoch(metadataOutput, "min_month");
        Path batchMetadataOutput = new Path(baseOutput, "batch_metadata");
        Job batchMetadataJob = createJob(
            "nasa-batch-metadata",
            BatchMetadataMapper.class,
            BatchMetadataReducer.class,
            batchMetadataOutput
        );
        batchMetadataJob.getConfiguration().set("nasa.batch.mode", batchMode);
        batchMetadataJob.getConfiguration().setLong("nasa.batch.value", batchMode.equals("calendar_month") ? 0L : batchValue);
        batchMetadataJob.getConfiguration().setLong("nasa.batch.first.epoch", firstEpoch);
        batchMetadataJob.getConfiguration().setLong("nasa.batch.first.year", firstYear);
        batchMetadataJob.getConfiguration().setLong("nasa.batch.first.month", firstMonth);
        batchMetadataJob.setMapOutputKeyClass(Text.class);
        batchMetadataJob.setMapOutputValueClass(Text.class);
        batchMetadataJob.setOutputKeyClass(Text.class);
        batchMetadataJob.setOutputValueClass(Text.class);
        batchMetadataJob.setNumReduceTasks(1);
        addInputs(batchMetadataJob, inputPaths);
        if (!batchMetadataJob.waitForCompletion(true)) {
            throw new IllegalStateException("Batch metadata MapReduce job failed");
        }

        List<BatchStat> batchStats = readBatchStats(batchMetadataOutput);
        long totalBatches = batchStats.size();

        Path q1Output = null;
        if ("all".equals(query) || "q1".equals(query)) {
            q1Output = new Path(baseOutput, "q1_daily_traffic");
            Job q1Job = createJob("nasa-q1-daily-traffic", DailyTrafficMapper.class, DailyTrafficReducer.class, q1Output);
            q1Job.getConfiguration().set("nasa.aggregation.mode", aggregationMode);
            q1Job.getConfiguration().setLong("nasa.result.batch.id", totalBatches);
            q1Job.setMapOutputKeyClass(Text.class);
            q1Job.setMapOutputValueClass(Text.class);
            q1Job.setOutputKeyClass(Text.class);
            q1Job.setOutputValueClass(Text.class);
            q1Job.setCombinerClass(DailyTrafficReducer.class);
            FileInputFormat.addInputPath(q1Job, batchMetadataOutput);
            if (!q1Job.waitForCompletion(true)) {
                throw new IllegalStateException("Q1 MapReduce job failed");
            }
        }

        Path q2Output = null;
        if ("all".equals(query) || "q2".equals(query)) {
            q2Output = new Path(baseOutput, "q2_top_resources");
            Job q2Job = createJob("nasa-q2-top-resources", TopResourcesMapper.class, TopResourcesReducer.class, q2Output);
            q2Job.getConfiguration().set("nasa.aggregation.mode", aggregationMode);
            q2Job.getConfiguration().setLong("nasa.result.batch.id", totalBatches);
            q2Job.setMapOutputKeyClass(Text.class);
            q2Job.setMapOutputValueClass(Text.class);
            q2Job.setOutputKeyClass(Text.class);
            q2Job.setOutputValueClass(Text.class);
            q2Job.setNumReduceTasks(1);
            FileInputFormat.addInputPath(q2Job, batchMetadataOutput);
            if (!q2Job.waitForCompletion(true)) {
                throw new IllegalStateException("Q2 MapReduce job failed");
            }
        }

        Path q3Output = null;
        if ("all".equals(query) || "q3".equals(query)) {
            q3Output = new Path(baseOutput, "q3_hourly_errors");
            Job q3Job = createJob("nasa-q3-hourly-errors", HourlyErrorsMapper.class, HourlyErrorsReducer.class, q3Output);
            q3Job.getConfiguration().set("nasa.aggregation.mode", aggregationMode);
            q3Job.getConfiguration().setLong("nasa.result.batch.id", totalBatches);
            q3Job.setMapOutputKeyClass(Text.class);
            q3Job.setMapOutputValueClass(Text.class);
            q3Job.setOutputKeyClass(Text.class);
            q3Job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(q3Job, batchMetadataOutput);
            if (!q3Job.waitForCompletion(true)) {
                throw new IllegalStateException("Q3 MapReduce job failed");
            }
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
            batchStats,
            aggregationMode,
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

    private long readMinEpoch(Path metadataOutput, String keyName) throws IOException {
        for (String line : readPartLines(metadataOutput)) {
            String[] parts = line.split("\\t", -1);
            if (parts.length >= 2 && keyName.equals(parts[0])) {
                return Long.parseLong(parts[1]);
            }
        }
        return 0L;
    }

    private List<BatchStat> readBatchStats(Path outputPath) throws IOException {
        Map<Long, BatchStat> stats = new TreeMap<Long, BatchStat>();
        for (String line : readPartLines(outputPath)) {
            String[] parts = line.split("\\t", -1);
            if (parts.length < 2) {
                continue;
            }
            if (parts[0].startsWith("B|")) {
                long batchId = Long.parseLong(parts[0].substring(2));
                BatchStat stat = stats.get(batchId);
                if (stat == null) {
                    stat = new BatchStat(batchId);
                    stats.put(batchId, stat);
                }
                if (parts.length >= 4) {
                    stat.recordsProcessed = Long.parseLong(parts[2]);
                    stat.malformedCount = Long.parseLong(parts[3]);
                }
            } else if (parts[0].startsWith("M|")) {
                long batchId = Long.parseLong(parts[0].substring(2));
                BatchStat stat = stats.get(batchId);
                if (stat == null) {
                    stat = new BatchStat(batchId);
                    stats.put(batchId, stat);
                }
                stat.malformedLines.add(parts[1]);
            }
        }
        return new ArrayList<BatchStat>(stats.values());
    }

    private void loadPostgres(String runUuid, String batchMode, int batchValue,
            long totalRecords, long totalBatches, double avgBatchSize, long malformedRecords,
            double runtimeSeconds, List<BatchStat> batchStats, String aggregationMode,
            Path q1Output, Path q2Output, Path q3Output) throws Exception {
        StringBuilder sql = new StringBuilder();
        sql.append(schemaSql());
        sql.append("INSERT INTO etl_runs ")
            .append("(run_uuid, pipeline, batch_size, batch_mode, batch_interval_seconds, aggregation_mode, status, started_at) VALUES (")
            .append(sqlString(runUuid)).append(", ")
            .append("'mapreduce', ")
            .append("records".equals(batchMode) ? String.valueOf(batchValue) : "NULL").append(", ")
            .append(sqlString(batchMode)).append(", ")
            .append("time".equals(batchMode) ? String.valueOf(batchValue) : "NULL").append(", ")
            .append(sqlString(aggregationMode)).append(", ")
            .append("'running', NOW()) ")
            .append("ON CONFLICT (run_uuid) DO UPDATE SET ")
            .append("pipeline = EXCLUDED.pipeline, ")
            .append("batch_size = EXCLUDED.batch_size, ")
            .append("batch_mode = EXCLUDED.batch_mode, ")
            .append("aggregation_mode = EXCLUDED.aggregation_mode, ")
            .append("batch_interval_seconds = EXCLUDED.batch_interval_seconds, ")
            .append("status = EXCLUDED.status;\n");
        sql.append("DELETE FROM daily_traffic WHERE run_uuid = ").append(sqlString(runUuid)).append(";\n");
        sql.append("DELETE FROM top_resources WHERE run_uuid = ").append(sqlString(runUuid)).append(";\n");
        sql.append("DELETE FROM hourly_errors WHERE run_uuid = ").append(sqlString(runUuid)).append(";\n");
        sql.append("DELETE FROM batch_metadata WHERE run_uuid = ").append(sqlString(runUuid)).append(";\n");
        sql.append("DELETE FROM malformed_record_summary WHERE run_uuid = ").append(sqlString(runUuid)).append(";\n");
        sql.append("DELETE FROM malformed_records WHERE run_uuid = ").append(sqlString(runUuid)).append(";\n");

        appendBatchMetadata(sql, runUuid, batchValue, batchStats, malformedRecords);
        if (q1Output != null) {
            appendQ1Inserts(sql, runUuid, totalBatches, aggregationMode, q1Output);
        }
        if (q2Output != null) {
            appendQ2Inserts(sql, runUuid, totalBatches, aggregationMode, q2Output);
        }
        if (q3Output != null) {
            appendQ3Inserts(sql, runUuid, totalBatches, aggregationMode, q3Output);
        }

        sql.append("UPDATE etl_runs SET ")
            .append("total_records = ").append(totalRecords).append(", ")
            .append("total_batches = ").append(totalBatches).append(", ")
            .append("avg_batch_size = ").append(String.format(Locale.US, "%.4f", avgBatchSize)).append(", ")
            .append("malformed_count = ").append(malformedRecords).append(", ")
            .append("runtime_seconds = ").append(String.format(Locale.US, "%.4f", runtimeSeconds)).append(", ")
            .append("status = 'completed', ")
            .append("completed_at = NOW(), ")
            .append("batch_mode = ").append(sqlString(batchMode)).append(", ")
            .append("aggregation_mode = ").append(sqlString(aggregationMode)).append(", ")
            .append("batch_interval_seconds = ")
            .append("time".equals(batchMode) ? String.valueOf(batchValue) : "NULL")
            .append(" WHERE run_uuid = ").append(sqlString(runUuid)).append(";\n");

        runPsql(sql.toString());
    }

    private void appendBatchMetadata(StringBuilder sql, String runUuid, int batchValue,
            List<BatchStat> batchStats, long malformedRecords) {
        for (BatchStat stat : batchStats) {
            sql.append("INSERT INTO batch_metadata ")
                .append("(run_uuid, pipeline, batch_id, batch_size, records_processed, malformed_count) VALUES (")
                .append(sqlString(runUuid)).append(", 'mapreduce', ")
                .append(stat.batchId).append(", ")
                .append(batchValue).append(", ")
                .append(stat.recordsProcessed).append(", ")
                .append(stat.malformedCount).append(");\n");
            if (stat.malformedCount > 0) {
                sql.append("INSERT INTO malformed_record_summary ")
                    .append("(run_uuid, pipeline, batch_id, malformed_count) VALUES (")
                    .append(sqlString(runUuid)).append(", 'mapreduce', ")
                    .append(stat.batchId).append(", ")
                    .append(stat.malformedCount).append(");\n");
            }
            for (String rawLine : stat.malformedLines) {
                sql.append("INSERT INTO malformed_records ")
                    .append("(run_uuid, pipeline, batch_id, raw_line, reason) VALUES (")
                    .append(sqlString(runUuid)).append(", 'mapreduce', ")
                    .append(stat.batchId).append(", ")
                    .append(sqlString(rawLine)).append(", 'parse_failed');\n");
            }
        }
        if (malformedRecords > 0 && batchStats.isEmpty()) {
            sql.append("INSERT INTO malformed_record_summary ")
                .append("(run_uuid, pipeline, batch_id, malformed_count) VALUES (")
                .append(sqlString(runUuid)).append(", 'mapreduce', 0, ")
                .append(malformedRecords).append(");\n");
        }
    }

    private void appendQ1Inserts(StringBuilder sql, String runUuid, long batchId, String aggregationMode, Path outputPath)
            throws IOException {
        for (String line : readPartLines(outputPath)) {
            String[] parts = line.split("\\t", -1);
            if (parts.length < 3) {
                continue;
            }
            String[] keyParts = parts[0].split("\\|", -1);
            if (keyParts.length < 3) {
                continue;
            }
            long resultBatchId = "per_batch".equals(aggregationMode) ? Long.parseLong(keyParts[0]) : batchId;
            sql.append("INSERT INTO daily_traffic ")
                .append("(pipeline, run_uuid, batch_id, log_date, status_code, request_count, total_bytes) VALUES (")
                .append("'mapreduce', ")
                .append(sqlString(runUuid)).append(", ")
                .append(resultBatchId).append(", ")
                .append(sqlString(keyParts[1])).append(", ")
                .append(keyParts[2]).append(", ")
                .append(parts[1]).append(", ")
                .append(parts[2]).append(");\n");
        }
    }

    private void appendQ2Inserts(StringBuilder sql, String runUuid, long batchId, String aggregationMode, Path outputPath)
            throws IOException {
        for (String line : readPartLines(outputPath)) {
            String[] parts = line.split("\\t", -1);
            if (parts.length < 4) {
                continue;
            }
            String[] keyParts = parts[0].split("\\|", -1);
            if (keyParts.length < 2) {
                continue;
            }
            long resultBatchId = "per_batch".equals(aggregationMode) ? Long.parseLong(keyParts[0]) : batchId;
            sql.append("INSERT INTO top_resources ")
                .append("(pipeline, run_uuid, batch_id, resource_path, request_count, total_bytes, distinct_host_count) VALUES (")
                .append("'mapreduce', ")
                .append(sqlString(runUuid)).append(", ")
                .append(resultBatchId).append(", ")
                .append(sqlString(keyParts[1])).append(", ")
                .append(parts[1]).append(", ")
                .append(parts[2]).append(", ")
                .append(parts[3]).append(");\n");
        }
    }

    private void appendQ3Inserts(StringBuilder sql, String runUuid, long batchId, String aggregationMode, Path outputPath)
            throws IOException {
        for (String line : readPartLines(outputPath)) {
            String[] parts = line.split("\\t", -1);
            if (parts.length < 5) {
                continue;
            }
            String[] keyParts = parts[0].split("\\|", -1);
            if (keyParts.length < 3) {
                continue;
            }
            long resultBatchId = "per_batch".equals(aggregationMode) ? Long.parseLong(keyParts[0]) : batchId;
            sql.append("INSERT INTO hourly_errors ")
                .append("(pipeline, run_uuid, batch_id, log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts) VALUES (")
                .append("'mapreduce', ")
                .append(sqlString(runUuid)).append(", ")
                .append(resultBatchId).append(", ")
                .append(sqlString(keyParts[1])).append(", ")
                .append(keyParts[2]).append(", ")
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
            + "ALTER TABLE etl_runs ADD COLUMN IF NOT EXISTS aggregation_mode VARCHAR(20) DEFAULT 'global';\n"
            + "CREATE TABLE IF NOT EXISTS batch_metadata ("
            + "id SERIAL PRIMARY KEY, run_uuid VARCHAR(64), pipeline VARCHAR(20), batch_id INTEGER, "
            + "batch_size INTEGER, records_processed INTEGER, malformed_count INTEGER DEFAULT 0, "
            + "started_at TIMESTAMPTZ DEFAULT NOW(), completed_at TIMESTAMPTZ DEFAULT NOW()"
            + ");\n"
            + "CREATE TABLE IF NOT EXISTS malformed_record_summary ("
            + "id SERIAL PRIMARY KEY, run_uuid VARCHAR(64), pipeline VARCHAR(20), batch_id INTEGER, "
            + "malformed_count INTEGER, recorded_at TIMESTAMPTZ DEFAULT NOW()"
            + ");\n"
            + "CREATE TABLE IF NOT EXISTS malformed_records ("
            + "id SERIAL PRIMARY KEY, run_uuid VARCHAR(64), pipeline VARCHAR(20), batch_id INTEGER, "
            + "raw_line TEXT, reason TEXT, recorded_at TIMESTAMPTZ DEFAULT NOW()"
            + ");\n"
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
            + ");\n"
            + "CREATE INDEX IF NOT EXISTS idx_batch_run ON batch_metadata(run_uuid, batch_id);\n"
            + "CREATE INDEX IF NOT EXISTS idx_malformed_summary_run ON malformed_record_summary(run_uuid, batch_id);\n"
            + "CREATE INDEX IF NOT EXISTS idx_malformed_records_run ON malformed_records(run_uuid, batch_id);\n"
            + "CREATE INDEX IF NOT EXISTS idx_daily_pipeline_date ON daily_traffic(pipeline, log_date);\n"
            + "CREATE INDEX IF NOT EXISTS idx_resource_pipeline ON top_resources(pipeline, request_count DESC);\n"
            + "CREATE INDEX IF NOT EXISTS idx_error_pipeline_date ON hourly_errors(pipeline, log_date, log_hour);\n";
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
