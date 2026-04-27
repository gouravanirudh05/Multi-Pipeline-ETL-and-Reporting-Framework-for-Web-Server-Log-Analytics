// mongodb_pipeline.js
// Phase 1 MongoDB ETL Pipeline (Fixed Version)
// Run:
// npm init -y
// npm install mongodb pg
// package.json -> "type": "module"
// node mongodb_pipeline.js <log_file_path> <batch_size>

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
    CREATE TABLE IF NOT EXISTS pipeline_runs (
      run_id SERIAL PRIMARY KEY,
      pipeline_name VARCHAR(50),
      batch_size INT,
      total_batches INT,
      avg_batch_size FLOAT,
      malformed_records INT,
      runtime_seconds FLOAT,
      execution_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS daily_traffic_summary (
      run_id INT,
      batch_id INT,
      log_date DATE,
      status_code INT,
      request_count BIGINT,
      total_bytes BIGINT
    );
  `);

  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS top_resources (
      run_id INT,
      batch_id INT,
      resource_path TEXT,
      request_count BIGINT,
      total_bytes BIGINT,
      distinct_host_count BIGINT
    );
  `);

  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS hourly_error_analysis (
      run_id INT,
      batch_id INT,
      log_date DATE,
      log_hour INT,
      error_request_count BIGINT,
      total_request_count BIGINT,
      error_rate FLOAT,
      distinct_error_hosts BIGINT
    );
  `);
}

// ----------------------
// Process one batch
// ----------------------
async function processBatch(
  batch,
  batchId,
  runId,
  logsCollection,
  pgClient
) {
  console.log(`Processing Batch ${batchId} (${batch.length} rows)`);

  // Clear previous batch
  await logsCollection.deleteMany({});

  // Insert current batch
  await logsCollection.insertMany(batch);

  // ----------------------
  // Query 1: Daily Traffic Summary
  // ----------------------
  const dailySummary = await logsCollection
    .aggregate([
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
    ])
    .toArray();

  for (const row of dailySummary) {
    await pgClient.query(
      `
      INSERT INTO daily_traffic_summary
      (run_id, batch_id, log_date, status_code, request_count, total_bytes)
      VALUES ($1,$2,$3,$4,$5,$6)
      `,
      [
        runId,
        batchId,
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
  const topResources = await logsCollection
    .aggregate([
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
      { $sort: { request_count: -1 } },
      { $limit: 20 },
    ])
    .toArray();

  for (const row of topResources) {
    await pgClient.query(
      `
      INSERT INTO top_resources
      (run_id, batch_id, resource_path, request_count,
       total_bytes, distinct_host_count)
      VALUES ($1,$2,$3,$4,$5,$6)
      `,
      [
        runId,
        batchId,
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
  const hourlyErrors = await logsCollection
    .aggregate([
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
    ])
    .toArray();

  for (const row of hourlyErrors) {
    await pgClient.query(
      `
      INSERT INTO hourly_error_analysis
      (run_id, batch_id, log_date, log_hour,
       error_request_count, total_request_count,
       error_rate, distinct_error_hosts)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
      `,
      [
        runId,
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

// ----------------------
// Main Pipeline
// ----------------------
async function runPipeline(logFilePath, batchSize) {
  const startTime = Date.now();

  // MongoDB
  const mongoClient = new MongoClient(MONGO_URI);
  await mongoClient.connect();
  const mongoDb = mongoClient.db("nasa_logs");
  const logsCollection = mongoDb.collection("processed_logs");

  // PostgreSQL
  const pgClient = new Client(PG_CONFIG);
  await pgClient.connect();
  await initializePostgres(pgClient);

  let batch = [];
  let batchId = 1;
  let malformedRecords = 0;
  let totalRecords = 0;
  let validRecords = 0;
  let totalBatches = 0;

  // Create pipeline run first
  const runInsert = await pgClient.query(
    `
    INSERT INTO pipeline_runs
    (pipeline_name, batch_size, total_batches,
     avg_batch_size, malformed_records, runtime_seconds)
    VALUES ($1,$2,0,0,0,0)
    RETURNING run_id
    `,
    ["MongoDB", batchSize]
  );

  const runId = runInsert.rows[0].run_id;

  const rl = readline.createInterface({
    input: fs.createReadStream(logFilePath),
    crlfDelay: Infinity,
  });

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
        runId,
        logsCollection,
        pgClient
      );

      totalBatches++;
      batchId++;
      batch = [];
    }
  }

  // Final batch
  if (batch.length > 0) {
    await processBatch(
      batch,
      batchId,
      runId,
      logsCollection,
      pgClient
    );

    totalBatches++;
  }

  const runtimeSeconds = (Date.now() - startTime) / 1000;
  const avgBatchSize =
    totalBatches > 0 ? validRecords / totalBatches : 0;

  // Update final metadata
  await pgClient.query(
    `
    UPDATE pipeline_runs
    SET total_batches = $1,
        avg_batch_size = $2,
        malformed_records = $3,
        runtime_seconds = $4
    WHERE run_id = $5
    `,
    [
      totalBatches,
      avgBatchSize,
      malformedRecords,
      runtimeSeconds,
      runId,
    ]
  );

  console.log("\n===== PIPELINE COMPLETE =====");
  console.log(`Run ID: ${runId}`);
  console.log(`Total Records: ${totalRecords}`);
  console.log(`Valid Records: ${validRecords}`);
  console.log(`Malformed Records: ${malformedRecords}`);
  console.log(`Total Batches: ${totalBatches}`);
  console.log(`Average Batch Size: ${avgBatchSize.toFixed(2)}`);
  console.log(`Runtime: ${runtimeSeconds.toFixed(2)} sec`);

  await mongoClient.close();
  await pgClient.end();
}

// ----------------------
// Entry Point
// ----------------------
const args = process.argv.slice(2);

if (args.length < 2) {
  console.log(
    "Usage: node mongodb_pipeline.js <log_file_path> <batch_size>"
  );
  process.exit(1);
}

const [logFilePath, batchSize] = args;

runPipeline(logFilePath, parseInt(batchSize)).catch((err) => {
  console.error("Pipeline failed:", err);
});