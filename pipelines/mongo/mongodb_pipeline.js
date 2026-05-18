// mongodb_pipeline.js
// MongoDB ETL Pipeline for NASA HTTP Log Analysis
// Usage:
// npm init -y
// npm install mongodb pg
// package.json -> "type": "module"
// node mongodb_pipeline.js <log_file_path_or_json_array> <batch_mode> <batch_value> <run_uuid>

import fs from "fs";
import readline from "readline";
import { MongoClient } from "mongodb";
import pg from "pg";

const { Client } = pg;

// ----------------------
// Environment Config
// ----------------------
const MONGO_URI = process.env.MONGO_URI || "mongodb://127.0.0.1:27017";

const PG_CONFIG = {
  connectionString:
    process.env.DATABASE_URL ||
    "postgresql://sathish:welcome@127.0.0.1:5432/nosql_etl_db",
};

// ----------------------
// Log Pattern
// ----------------------
const LOG_PATTERN =
  /^(\S+) \S+ \S+ \[(.*?)\] \"(\S+) (.*?) (\S+)\" (\d{3}) (\S+)/;

// ----------------------
// Month Mapping
// ----------------------
const MONTH_MAP = {
  Jan: "01",
  Feb: "02",
  Mar: "03",
  Apr: "04",
  May: "05",
  Jun: "06",
  Jul: "07",
  Aug: "08",
  Sep: "09",
  Oct: "10",
  Nov: "11",
  Dec: "12",
};

function parseTimestamp(timestamp) {
  const match = timestamp.match(
    /^(\d{2})\/([A-Za-z]{3})\/(\d{4}):(\d{2}):(\d{2}):(\d{2})(?:\s+([+-])(\d{2})(\d{2}))?$/
  );
  if (!match) return null;

  const [, day, month, year, hour, minute, second, offsetSign, offsetHour, offsetMinute] =
    match;
  const monthNumber = MONTH_MAP[month];
  if (!monthNumber) return null;

  const logHour = parseInt(hour, 10);
  const utcMillis = Date.UTC(
    parseInt(year, 10),
    parseInt(monthNumber, 10) - 1,
    parseInt(day, 10),
    logHour,
    parseInt(minute, 10),
    parseInt(second, 10)
  );

  let offsetMinutes = 0;
  if (offsetSign && offsetHour && offsetMinute) {
    offsetMinutes =
      parseInt(offsetHour, 10) * 60 + parseInt(offsetMinute, 10);
    if (offsetSign === "-") offsetMinutes *= -1;
  }

  return {
    log_date: `${year}-${monthNumber}-${day}`,
    log_hour: logHour,
    timestamp_epoch: Math.floor((utcMillis - offsetMinutes * 60000) / 1000),
  };
}

// ----------------------
// Parse a single log line
// ----------------------
function parseLogLine(line) {
  const match = line.match(LOG_PATTERN);
  if (!match) return null;

  const [
    ,
    host,
    timestamp,
    method,
    resourcePath,
    protocol,
    statusCode,
    bytesTransferred,
  ] = match;

  try {
    const parsedTimestamp = parseTimestamp(timestamp);
    const parsedStatus = parseInt(statusCode, 10);
    const parsedBytes =
      bytesTransferred === "-" ? 0 : parseInt(bytesTransferred, 10);

    if (
      !host ||
      !method ||
      !resourcePath ||
      !protocol ||
      !parsedTimestamp ||
      Number.isNaN(parsedStatus) ||
      Number.isNaN(parsedBytes)
    ) {
      return null;
    }

    return {
      host,
      timestamp,
      ...parsedTimestamp,
      method,
      resource_path: resourcePath,
      protocol,
      status_code: parsedStatus,
      bytes_transferred: parsedBytes,
    };
  } catch {
    return null;
  }
}

// ----------------------
// PostgreSQL schema initialization
// ----------------------
async function initializePostgres(pgClient) {
  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS etl_runs (
      run_uuid VARCHAR(64) PRIMARY KEY,
      pipeline VARCHAR(20),
      batch_size INTEGER,
      total_records INTEGER,
      total_batches INTEGER,
      avg_batch_size NUMERIC(10,2),
      malformed_count INTEGER,
      runtime_seconds NUMERIC(10,3),
      status VARCHAR(20),
      started_at TIMESTAMPTZ,
      completed_at TIMESTAMPTZ
    );
  `);

  await pgClient.query(`
    ALTER TABLE etl_runs
    ADD COLUMN IF NOT EXISTS batch_mode VARCHAR(20) DEFAULT 'records';
  `);

  await pgClient.query(`
    ALTER TABLE etl_runs
    ADD COLUMN IF NOT EXISTS batch_interval_seconds INTEGER;
  `);

  await pgClient.query(`
    ALTER TABLE etl_runs
    ADD COLUMN IF NOT EXISTS aggregation_mode VARCHAR(20) DEFAULT 'global';
  `);

  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS batch_metadata (
      id SERIAL PRIMARY KEY,
      run_uuid VARCHAR(64),
      pipeline VARCHAR(20),
      batch_id INTEGER,
      batch_size INTEGER,
      records_processed INTEGER,
      malformed_count INTEGER DEFAULT 0,
      started_at TIMESTAMPTZ DEFAULT NOW(),
      completed_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS malformed_record_summary (
      id SERIAL PRIMARY KEY,
      run_uuid VARCHAR(64),
      pipeline VARCHAR(20),
      batch_id INTEGER,
      malformed_count INTEGER,
      recorded_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS malformed_records (
      id SERIAL PRIMARY KEY,
      run_uuid VARCHAR(64),
      pipeline VARCHAR(20),
      batch_id INTEGER,
      raw_line TEXT,
      reason TEXT,
      recorded_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS daily_traffic (
      id SERIAL PRIMARY KEY,
      pipeline VARCHAR(20),
      run_uuid VARCHAR(64),
      batch_id INTEGER,
      executed_at TIMESTAMPTZ DEFAULT NOW(),
      log_date DATE,
      status_code INTEGER,
      request_count BIGINT,
      total_bytes BIGINT
    );
  `);

  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS top_resources (
      id SERIAL PRIMARY KEY,
      pipeline VARCHAR(20),
      run_uuid VARCHAR(64),
      batch_id INTEGER,
      executed_at TIMESTAMPTZ DEFAULT NOW(),
      resource_path TEXT,
      request_count BIGINT,
      total_bytes BIGINT,
      distinct_host_count BIGINT
    );
  `);

  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS hourly_errors (
      id SERIAL PRIMARY KEY,
      pipeline VARCHAR(20),
      run_uuid VARCHAR(64),
      batch_id INTEGER,
      executed_at TIMESTAMPTZ DEFAULT NOW(),
      log_date DATE,
      log_hour SMALLINT,
      error_request_count BIGINT,
      total_request_count BIGINT,
      error_rate NUMERIC(6,4),
      distinct_error_hosts BIGINT
    );
  `);

  await pgClient.query(`CREATE INDEX IF NOT EXISTS idx_batch_run ON batch_metadata(run_uuid, batch_id);`);
  await pgClient.query(`CREATE INDEX IF NOT EXISTS idx_malformed_summary_run ON malformed_record_summary(run_uuid, batch_id);`);
  await pgClient.query(`CREATE INDEX IF NOT EXISTS idx_malformed_records_run ON malformed_records(run_uuid, batch_id);`);
  await pgClient.query(`CREATE INDEX IF NOT EXISTS idx_daily_pipeline_date ON daily_traffic(pipeline, log_date);`);
  await pgClient.query(`CREATE INDEX IF NOT EXISTS idx_resource_pipeline ON top_resources(pipeline, request_count DESC);`);
  await pgClient.query(`CREATE INDEX IF NOT EXISTS idx_error_pipeline_date ON hourly_errors(pipeline, log_date, log_hour);`);
}

// ----------------------
// Insert one parsed batch into MongoDB
// ----------------------
async function processBatch(
  batch,
  batchId,
  runUuid,
  logsCollection
) {
  console.log(`[Batch ${batchId}] Loading ${batch.length} records...`);

  const rows = batch.map((row) => ({
    ...row,
    run_uuid: runUuid,
    batch_id: batchId,
  }));

  await logsCollection.insertMany(rows);
  console.log(`[Batch ${batchId}] Loaded`);
}

// ----------------------
// Run required analytics over the full run
// ----------------------
async function writeFinalAggregates(
  runUuid,
  pipeline,
  totalBatches,
  logsCollection,
  pgClient,
  query,
  aggregationMode
) {
  const resultBatchId = totalBatches;
  const batchIds =
    aggregationMode === "per_batch"
      ? Array.from({ length: totalBatches }, (_, index) => index + 1)
      : [resultBatchId];

  if (query === "all" || query === "q1") {
    await pgClient.query("DELETE FROM daily_traffic WHERE run_uuid = $1", [
      runUuid,
    ]);
  }
  if (query === "all" || query === "q2") {
    await pgClient.query("DELETE FROM top_resources WHERE run_uuid = $1", [
      runUuid,
    ]);
  }
  if (query === "all" || query === "q3") {
    await pgClient.query("DELETE FROM hourly_errors WHERE run_uuid = $1", [
      runUuid,
    ]);
  }

  // ----------------------
  // Query 1: Daily Traffic Summary
  // ----------------------
  if (query === "all" || query === "q1") {
    console.log(
      aggregationMode === "per_batch"
        ? "Running Q1 (Daily Traffic) per batch..."
        : "Running Q1 (Daily Traffic) over full run..."
    );
    for (const batchId of batchIds) {
      const dailySummary = await logsCollection
        .aggregate(
          [
            {
              $match:
                aggregationMode === "per_batch"
                  ? { run_uuid: runUuid, batch_id: batchId }
                  : { run_uuid: runUuid },
            },
            {
              $group: {
                _id: {
                  log_date: "$log_date",
                  status_code: "$status_code",
                },
                request_count: { $sum: 1 },
                total_bytes: { $sum: "$bytes_transferred" },
              },
            },
          ],
          { allowDiskUse: true }
        )
        .toArray();

      for (const row of dailySummary) {
        await pgClient.query(
          `
          INSERT INTO daily_traffic
          (pipeline, run_uuid, batch_id, log_date, status_code, request_count, total_bytes)
          VALUES ($1, $2, $3, $4, $5, $6, $7)
          `,
          [
            pipeline,
            runUuid,
            batchId,
            row._id.log_date,
            row._id.status_code,
            row.request_count,
            row.total_bytes,
          ]
        );
      }
    }
  }

  // ----------------------
  // Query 2: Top Requested Resources
  // ----------------------
  if (query === "all" || query === "q2") {
    console.log(
      aggregationMode === "per_batch"
        ? "Running Q2 (Top Resources) per batch..."
        : "Running Q2 (Top Resources) over full run..."
    );
    for (const batchId of batchIds) {
      const topResources = await logsCollection
        .aggregate(
          [
            {
              $match:
                aggregationMode === "per_batch"
                  ? { run_uuid: runUuid, batch_id: batchId }
                  : { run_uuid: runUuid },
            },
            {
              $group: {
                _id: "$resource_path",
                request_count: { $sum: 1 },
                total_bytes: { $sum: "$bytes_transferred" },
                distinct_hosts: { $addToSet: "$host" },
              },
            },
            {
              $project: {
                resource_path: "$_id",
                request_count: 1,
                total_bytes: 1,
                distinct_host_count: { $size: "$distinct_hosts" },
              },
            },
            { $sort: { request_count: -1, resource_path: 1 } },
            { $limit: 20 },
          ],
          { allowDiskUse: true }
        )
        .toArray();

      for (const row of topResources) {
        await pgClient.query(
          `
          INSERT INTO top_resources
          (pipeline, run_uuid, batch_id, resource_path, request_count, total_bytes, distinct_host_count)
          VALUES ($1, $2, $3, $4, $5, $6, $7)
          `,
          [
            pipeline,
            runUuid,
            batchId,
            row.resource_path,
            row.request_count,
            row.total_bytes,
            row.distinct_host_count,
          ]
        );
      }
    }
  }

  // ----------------------
  // Query 3: Hourly Error Analysis
  // ----------------------
  if (query === "all" || query === "q3") {
    console.log(
      aggregationMode === "per_batch"
        ? "Running Q3 (Hourly Errors) per batch..."
        : "Running Q3 (Hourly Errors) over full run..."
    );
    for (const batchId of batchIds) {
      const hourlyErrors = await logsCollection
        .aggregate(
          [
            {
              $match:
                aggregationMode === "per_batch"
                  ? { run_uuid: runUuid, batch_id: batchId }
                  : { run_uuid: runUuid },
            },
            {
              $group: {
                _id: {
                  log_date: "$log_date",
                  log_hour: "$log_hour",
                },
                total_requests: { $sum: 1 },
                error_requests: {
                  $sum: {
                    $cond: [
                      {
                        $and: [
                          { $gte: ["$status_code", 400] },
                          { $lte: ["$status_code", 599] },
                        ],
                      },
                      1,
                      0,
                    ],
                  },
                },
                error_hosts: {
                  $addToSet: {
                    $cond: [
                      {
                        $and: [
                          { $gte: ["$status_code", 400] },
                          { $lte: ["$status_code", 599] },
                        ],
                      },
                      "$host",
                      "$$REMOVE",
                    ],
                  },
                },
              },
            },
            {
              $project: {
                log_date: "$_id.log_date",
                log_hour: "$_id.log_hour",
                total_requests: 1,
                error_requests: 1,
                error_rate: {
                  $cond: [
                    { $eq: ["$total_requests", 0] },
                    0,
                    {
                      $divide: ["$error_requests", "$total_requests"],
                    },
                  ],
                },
                distinct_error_hosts: {
                  $size: "$error_hosts",
                },
              },
            },
          ],
          { allowDiskUse: true }
        )
        .toArray();

      for (const row of hourlyErrors) {
        await pgClient.query(
          `
          INSERT INTO hourly_errors
          (pipeline, run_uuid, batch_id, log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
          `,
          [
            pipeline,
            runUuid,
            batchId,
            row.log_date,
            row.log_hour,
            row.error_requests,
            row.total_requests,
            row.error_rate,
            row.distinct_error_hosts,
          ]
        );
      }
    }
  }

  console.log(
    `Final aggregates written to PostgreSQL after ${totalBatches} source batches`
  );
}

async function writeBatchMetadata(pgClient, runUuid, pipeline, batchValue, batchStats) {
  await pgClient.query("DELETE FROM batch_metadata WHERE run_uuid = $1", [runUuid]);
  await pgClient.query("DELETE FROM malformed_record_summary WHERE run_uuid = $1", [runUuid]);
  await pgClient.query("DELETE FROM malformed_records WHERE run_uuid = $1", [runUuid]);

  const batchIds = [...batchStats.keys()].sort((a, b) => a - b);
  for (const batchId of batchIds) {
    const stats = batchStats.get(batchId);
    await pgClient.query(
      `
      INSERT INTO batch_metadata
        (run_uuid, pipeline, batch_id, batch_size, records_processed, malformed_count)
      VALUES ($1, $2, $3, $4, $5, $6)
      `,
      [runUuid, pipeline, batchId, batchValue, stats.records, stats.malformed]
    );
    if (stats.malformed > 0) {
      await pgClient.query(
        `
        INSERT INTO malformed_record_summary
          (run_uuid, pipeline, batch_id, malformed_count)
        VALUES ($1, $2, $3, $4)
        `,
        [runUuid, pipeline, batchId, stats.malformed]
      );
    }
    for (const rawLine of stats.malformedLines || []) {
      await pgClient.query(
        `
        INSERT INTO malformed_records
          (run_uuid, pipeline, batch_id, raw_line, reason)
        VALUES ($1, $2, $3, $4, $5)
        `,
        [runUuid, pipeline, batchId, rawLine, "parse_failed"]
      );
    }
  }
}

// ----------------------
// Main Pipeline
// ----------------------
function parseLogFilePaths(rawArg) {
  try {
    const parsed = JSON.parse(rawArg);
    if (Array.isArray(parsed)) return parsed;
  } catch {
    // Keep backward-compatible single-file CLI usage.
  }

  return [rawArg];
}

function createTimeBatchAssigner(intervalSeconds) {
  return {
    firstEpoch: null,
    windows: new Map(),
    assign(epochSeconds) {
      if (this.firstEpoch === null) this.firstEpoch = epochSeconds;
      const windowIndex = Math.floor(
        (epochSeconds - this.firstEpoch) / intervalSeconds
      );
      if (!this.windows.has(windowIndex)) {
        this.windows.set(windowIndex, this.windows.size + 1);
      }
      return this.windows.get(windowIndex);
    },
  };
}

async function runPipeline(
  logFilePaths,
  batchMode,
  batchValue,
  runUuid,
  pipeline,
  query = "all",
  aggregationMode = "global"
) {
  const startTime = Date.now();
  const isTimeBatching = batchMode === "time";
  const batchLabel = isTimeBatching
    ? `${batchValue} seconds`
    : `${batchValue} records`;

  console.log(`
╔════════════════════════════════════════════════════════════╗
║              MongoDB ETL Pipeline Started                  ║
║  Run UUID: ${runUuid}
║  Batch Mode: ${batchMode}
║  Batch Unit: ${batchLabel}
╚════════════════════════════════════════════════════════════╝
  `);

  // MongoDB
  const mongoClient = new MongoClient(MONGO_URI);
  await mongoClient.connect();
  const mongoDb = mongoClient.db("nasa_logs");
  const logsCollection = mongoDb.collection("processed_logs");

  // PostgreSQL
  const pgClient = new Client(PG_CONFIG);
  await pgClient.connect();
  await initializePostgres(pgClient);
  await pgClient.query(
    `
    INSERT INTO etl_runs
      (run_uuid, pipeline, batch_size, batch_mode, batch_interval_seconds, aggregation_mode, status, started_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
    ON CONFLICT (run_uuid) DO UPDATE
    SET pipeline = EXCLUDED.pipeline,
        batch_size = EXCLUDED.batch_size,
        batch_mode = EXCLUDED.batch_mode,
        batch_interval_seconds = EXCLUDED.batch_interval_seconds,
        aggregation_mode = EXCLUDED.aggregation_mode,
        status = EXCLUDED.status
    `,
    [
      runUuid,
      pipeline,
      isTimeBatching ? null : batchValue,
      batchMode,
      isTimeBatching ? batchValue : null,
      aggregationMode,
      "running",
    ]
  );

  // Clear any stale records for this run UUID before loading.
  await logsCollection.deleteMany({ run_uuid: runUuid });

  let batch = [];
  let activeBatchId = 1;
  let malformedRecords = 0;
  let totalRecords = 0;
  let validRecords = 0;
  let totalBatches = 0;
  const timeBatcher = createTimeBatchAssigner(batchValue);
  const batchStats = new Map();

  function ensureBatchStats(id) {
    if (!batchStats.has(id)) {
      batchStats.set(id, { records: 0, malformed: 0 });
    }
    return batchStats.get(id);
  }

  console.log(`Files to process: ${logFilePaths.length}`);

  for (const logFilePath of logFilePaths) {
    if (!fs.existsSync(logFilePath)) {
      throw new Error(`Log file not found: ${logFilePath}`);
    }

    const rl = readline.createInterface({
      input: fs.createReadStream(logFilePath),
      crlfDelay: Infinity,
    });

    console.log(`Reading from: ${logFilePath}`);

    for await (const line of rl) {
      totalRecords++;
      let rawBatchId = isTimeBatching
        ? activeBatchId
        : Math.floor((totalRecords - 1) / batchValue) + 1;

      const parsed = parseLogLine(line);

      if (isTimeBatching && parsed) {
        rawBatchId = timeBatcher.assign(parsed.timestamp_epoch);
      }

      const stats = ensureBatchStats(rawBatchId);
      stats.records++;

      if (!parsed) {
        malformedRecords++;
        stats.malformed++;
        if (!stats.malformedLines) stats.malformedLines = [];
        stats.malformedLines.push(line);
        continue;
      }

      validRecords++;
      const parsedBatchId = rawBatchId;

      if (
        batch.length > 0 &&
        parsedBatchId !== activeBatchId
      ) {
        await processBatch(
          batch,
          activeBatchId,
          runUuid,
          logsCollection
        );
        totalBatches++;
        batch = [];
      }

      activeBatchId = parsedBatchId;
      batch.push(parsed);
    }
  }

  // Final batch
  if (batch.length > 0) {
    await processBatch(
      batch,
      activeBatchId,
      runUuid,
      logsCollection
    );

    totalBatches++;
  }
  totalBatches = Math.max(totalBatches, batchStats.size);

  await writeBatchMetadata(pgClient, runUuid, pipeline, batchValue, batchStats);

  await writeFinalAggregates(
    runUuid,
    pipeline,
    totalBatches,
    logsCollection,
    pgClient,
    query,
    aggregationMode
  );

  const runtimeSeconds = (Date.now() - startTime) / 1000;
  const avgBatchSize =
    totalBatches > 0 ? totalRecords / totalBatches : 0;

  // Update final metadata
  await pgClient.query(
    `
    UPDATE etl_runs
    SET total_records = $1,
        total_batches = $2,
        avg_batch_size = $3,
        malformed_count = $4,
        runtime_seconds = $5,
        status = $6,
        completed_at = NOW(),
        batch_mode = $7,
        batch_interval_seconds = $8,
        aggregation_mode = $9
    WHERE run_uuid = $10
    `,
    [
      totalRecords,
      totalBatches,
      avgBatchSize,
      malformedRecords,
      runtimeSeconds,
      "completed",
      batchMode,
      isTimeBatching ? batchValue : null,
      aggregationMode,
      runUuid,
    ]
  );

  console.log(`
╔════════════════════════════════════════════════════════════╗
║                  PIPELINE COMPLETED ✓                      ║
╠════════════════════════════════════════════════════════════╣
║  Total Records:      ${String(totalRecords).padEnd(40, ' ')}║
║  Valid Records:      ${String(validRecords).padEnd(40, ' ')}║
║  Malformed:          ${String(malformedRecords).padEnd(40, ' ')}║
║  Total Batches:      ${String(totalBatches).padEnd(40, ' ')}║
║  Batch Mode:         ${String(batchMode).padEnd(40, ' ')}║
║  Avg Batch Size:     ${String(avgBatchSize.toFixed(2)).padEnd(40, ' ')}║
║  Runtime:            ${String(runtimeSeconds.toFixed(2) + ' sec').padEnd(40, ' ')}║
╚════════════════════════════════════════════════════════════╝
  `);

  await mongoClient.close();
  await pgClient.end();
}

// ----------------------
// Entry Point
// ----------------------
const args = process.argv.slice(2);

if (args.length < 3) {
  console.error(
    "Usage: node mongodb_pipeline.js <log_file_path_or_json_array> <batch_mode> <batch_value> <run_uuid>"
  );
  process.exit(1);
}

let logFilePathArg;
let batchMode;
let batchValue;
let runUuid;
let query = "all";
let aggregationMode = "global";

if (args.length === 3) {
  [logFilePathArg, batchValue, runUuid] = args;
  batchMode = "records";
} else {
  [logFilePathArg, batchMode, batchValue, runUuid, query = "all", aggregationMode = "global"] = args;
}

if (!["records", "time"].includes(batchMode)) {
  console.error("batch_mode must be either records or time");
  process.exit(1);
}
if (!["all", "q1", "q2", "q3"].includes(query)) {
  console.error("query must be one of: all, q1, q2, q3");
  process.exit(1);
}
if (!["global", "per_batch"].includes(aggregationMode)) {
  console.error("aggregation_mode must be one of: global, per_batch");
  process.exit(1);
}

const parsedBatchValue = parseInt(batchValue, 10);
if (!Number.isInteger(parsedBatchValue) || parsedBatchValue <= 0) {
  console.error("batch_value must be a positive integer");
  process.exit(1);
}

const logFilePaths = parseLogFilePaths(logFilePathArg);
const pipeline = "mongodb";

runPipeline(
  logFilePaths,
  batchMode,
  parsedBatchValue,
  runUuid,
  pipeline,
  query,
  aggregationMode
).catch(
  (err) => {
    console.error("Pipeline failed:", err);
    process.exit(1);
  }
);
