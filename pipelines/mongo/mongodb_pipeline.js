// mongodb_pipeline.js
// MongoDB ETL Pipeline for NASA HTTP Log Analysis
// Usage:
// npm init -y
// npm install mongodb pg
// package.json -> "type": "module"
// node mongodb_pipeline.js <log_file_path_or_json_array> <batch_size> <run_id> <run_uuid>

import fs from "fs";
import readline from "readline";
import { MongoClient } from "mongodb";
import pg from "pg";

const { Client } = pg;

// ----------------------
// Environment Config
// ----------------------
const MONGO_URI = "mongodb://127.0.0.1:27017";

const PG_CONFIG = {
  connectionString:
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
    const datePart = timestamp.split(" ")[0];
    const [day, month, rest] = datePart.split("/");
    const [year, hour] = rest.split(":");

    const logDate = `${year}-${MONTH_MAP[month]}-${day}`;

    return {
      host,
      timestamp,
      log_date: logDate,
      log_hour: parseInt(hour),
      method,
      resource_path: resourcePath,
      protocol,
      status_code: parseInt(statusCode),
      bytes_transferred:
        bytesTransferred === "-" ? 0 : parseInt(bytesTransferred),
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
      run_id SERIAL PRIMARY KEY,
      pipeline VARCHAR(20),
      run_uuid VARCHAR(64) UNIQUE,
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
    CREATE TABLE IF NOT EXISTS daily_traffic (
      id SERIAL PRIMARY KEY,
      run_id INTEGER,
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
      run_id INTEGER,
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
      run_id INTEGER,
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
  runId,
  runUuid,
  pipeline,
  totalBatches,
  logsCollection,
  pgClient
) {
  const resultBatchId = totalBatches;

  await pgClient.query("DELETE FROM daily_traffic WHERE run_uuid = $1", [
    runUuid,
  ]);
  await pgClient.query("DELETE FROM top_resources WHERE run_uuid = $1", [
    runUuid,
  ]);
  await pgClient.query("DELETE FROM hourly_errors WHERE run_uuid = $1", [
    runUuid,
  ]);

  // ----------------------
  // Query 1: Daily Traffic Summary
  // ----------------------
  console.log("Running Q1 (Daily Traffic) over full run...");
  const dailySummary = await logsCollection
    .aggregate(
      [
        { $match: { run_uuid: runUuid } },
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
      (run_id, pipeline, run_uuid, batch_id, log_date, status_code, request_count, total_bytes)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      `,
      [
        runId,
        pipeline,
        runUuid,
        resultBatchId,
        row._id.log_date,
        row._id.status_code,
        row.request_count,
        row.total_bytes,
      ]
    );
  }

  // ----------------------
  // Query 2: Top Requested Resources
  // ----------------------
  console.log("Running Q2 (Top Resources) over full run...");
  const topResources = await logsCollection
    .aggregate(
      [
        { $match: { run_uuid: runUuid } },
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
      (run_id, pipeline, run_uuid, batch_id, resource_path, request_count, total_bytes, distinct_host_count)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      `,
      [
        runId,
        pipeline,
        runUuid,
        resultBatchId,
        row.resource_path,
        row.request_count,
        row.total_bytes,
        row.distinct_host_count,
      ]
    );
  }

  // ----------------------
  // Query 3: Hourly Error Analysis
  // ----------------------
  console.log("Running Q3 (Hourly Errors) over full run...");
  const hourlyErrors = await logsCollection
    .aggregate(
      [
        { $match: { run_uuid: runUuid } },
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
      (run_id, pipeline, run_uuid, batch_id, log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      `,
      [
        runId,
        pipeline,
        runUuid,
        resultBatchId,
        row.log_date,
        row.log_hour,
        row.error_requests,
        row.total_requests,
        row.error_rate,
        row.distinct_error_hosts,
      ]
    );
  }

  console.log(
    `Final aggregates written to PostgreSQL after ${totalBatches} source batches`
  );
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

async function runPipeline(logFilePaths, batchSize, runId, runUuid, pipeline) {
  const startTime = Date.now();

  console.log(`
╔════════════════════════════════════════════════════════════╗
║              MongoDB ETL Pipeline Started                  ║
║  Run UUID: ${runUuid}
║  Batch Size: ${batchSize}
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

  // Clear any stale records for this run UUID before loading.
  await logsCollection.deleteMany({ run_uuid: runUuid });

  let batch = [];
  let batchId = 1;
  let malformedRecords = 0;
  let totalRecords = 0;
  let validRecords = 0;
  let totalBatches = 0;

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

      const parsed = parseLogLine(line);

      if (!parsed) {
        malformedRecords++;
        continue;
      }

      validRecords++;
      batch.push(parsed);

      if (batch.length >= batchSize) {
        await processBatch(
          batch,
          batchId,
          runUuid,
          logsCollection
        );

        totalBatches++;
        batchId++;
        batch = [];
      }
    }
  }

  // Final batch
  if (batch.length > 0) {
    await processBatch(
      batch,
      batchId,
      runUuid,
      logsCollection
    );

    totalBatches++;
  }

  await writeFinalAggregates(
    runId,
    runUuid,
    pipeline,
    totalBatches,
    logsCollection,
    pgClient
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
        completed_at = NOW()
    WHERE run_id = $7
    `,
    [
      totalRecords,
      totalBatches,
      avgBatchSize,
      malformedRecords,
      runtimeSeconds,
      "completed",
      runId,
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

if (args.length < 4) {
  console.error(
    "Usage: node mongodb_pipeline.js <log_file_path_or_json_array> <batch_size> <run_id> <run_uuid>"
  );
  process.exit(1);
}

const [logFilePathArg, batchSize, runIdArg, runUuid] = args;
const logFilePaths = parseLogFilePaths(logFilePathArg);
const runId = parseInt(runIdArg);
const pipeline = "mongodb";

runPipeline(logFilePaths, parseInt(batchSize), runId, runUuid, pipeline).catch(
  (err) => {
    console.error("Pipeline failed:", err);
    process.exit(1);
  }
);
