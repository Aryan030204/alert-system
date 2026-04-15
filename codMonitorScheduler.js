const os = require("os");
const path = require("path");
const { spawn } = require("child_process");
const { MongoClient } = require("mongodb");
const { processCodMonitorResults } = require("./alertEngine");

const SCHEDULE_TIME_ZONE = "Asia/Kolkata";
const LOCK_NAME = "cod_monitor_hourly";
const schedulerOwner = `${os.hostname()}:${process.pid}`;

let codMonitorTimer = null;
let codMonitorRunning = false;
let schedulerMongoClient = null;

function parseBoolean(value, fallback = false) {
  if (value === undefined || value === null || value === "") return fallback;
  return String(value).trim().toLowerCase() === "true";
}

function parseBrands(value) {
  if (!value) return [];
  return String(value)
    .split(",")
    .map((brand) => brand.trim())
    .filter(Boolean);
}

function getSchedulerConfig() {
  return {
    enabled: parseBoolean(process.env.COD_MONITOR_SCHEDULE_ENABLED, false),
    runOnBoot: parseBoolean(process.env.COD_MONITOR_RUN_ON_BOOT, false),
    dryRun: parseBoolean(process.env.COD_MONITOR_SCHEDULE_DRY_RUN, false),
    minute: Number(process.env.COD_MONITOR_SCHEDULE_MINUTE_IST || 0),
    pythonBin: process.env.COD_MONITOR_PYTHON_BIN || "python",
    scriptPath:
      process.env.COD_MONITOR_SCRIPT_PATH ||
      path.join(__dirname, "cod_monitor", "main.py"),
    brands: parseBrands(process.env.COD_MONITOR_BRANDS),
    leaseMinutes: Number(process.env.COD_MONITOR_SCHEDULE_LEASE_MINUTES || 50),
  };
}

function getNowInTimeZone(timeZone) {
  return new Date(new Date().toLocaleString("en-US", { timeZone }));
}

function getDelayUntilNextRun(minute, timeZone) {
  const now = getNowInTimeZone(timeZone);
  const next = new Date(now);
  next.setSeconds(0, 0);
  next.setMinutes(minute);
  if (next <= now) {
    next.setHours(next.getHours() + 1);
  }
  return next.getTime() - now.getTime();
}

function formatScheduledTime(delayMs, timeZone) {
  const target = new Date(Date.now() + delayMs);
  return new Intl.DateTimeFormat("en-IN", {
    timeZone,
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(target);
}

async function getSchedulerLockCollection() {
  if (schedulerMongoClient) {
    return schedulerMongoClient.db().collection("scheduler_locks");
  }

  if (!process.env.MONGO_URI) {
    throw new Error("MONGO_URI is required for COD scheduler locking");
  }

  schedulerMongoClient = new MongoClient(process.env.MONGO_URI);
  await schedulerMongoClient.connect();
  return schedulerMongoClient.db().collection("scheduler_locks");
}

async function acquireSchedulerLease(leaseMinutes) {
  const now = new Date();
  const lockedUntil = new Date(now.getTime() + leaseMinutes * 60 * 1000);
  const collection = await getSchedulerLockCollection();
  const result = await collection.findOneAndUpdate(
    {
      _id: LOCK_NAME,
      $or: [
        { lockedUntil: { $exists: false } },
        { lockedUntil: { $lte: now } },
        { owner: schedulerOwner },
      ],
    },
    {
      $set: {
        owner: schedulerOwner,
        lockedUntil,
        updatedAt: now,
      },
    },
    {
      upsert: true,
      returnDocument: "after",
    },
  );

  return result?.owner === schedulerOwner;
}

async function releaseSchedulerLease() {
  try {
    const collection = await getSchedulerLockCollection();
    await collection.updateOne(
      { _id: LOCK_NAME, owner: schedulerOwner },
      {
        $set: {
          lockedUntil: new Date(0),
          updatedAt: new Date(),
        },
      },
    );
  } catch (err) {
    console.warn("⚠️ Failed to release COD scheduler lease:", err.message);
  }
}

async function runCodMonitorJob() {
  if (codMonitorRunning) {
    console.log("⏭ COD monitor scheduler skipped: previous run still in progress.");
    return null;
  }

  const config = getSchedulerConfig();
  const leaseAcquired = await acquireSchedulerLease(config.leaseMinutes);
  if (!leaseAcquired) {
    console.log("⏭ COD monitor scheduler skipped: another instance owns the run lease.");
    return null;
  }

  const args = [config.scriptPath, "--json-output"];
  if (config.dryRun) args.push("--dry-run");
  if (config.brands.length) {
    args.push("--brands", ...config.brands);
  }

  codMonitorRunning = true;
  console.log(
    `🗓 Starting scheduled COD monitor run with ${config.pythonBin} ${args.join(" ")}`,
  );

  try {
    const result = await new Promise((resolve, reject) => {
      const child = spawn(config.pythonBin, args, {
        cwd: __dirname,
        env: process.env,
        windowsHide: true,
      });

      let stdout = "";
      let stderr = "";

      child.stdout.on("data", (chunk) => {
        stdout += chunk.toString();
      });

      child.stderr.on("data", (chunk) => {
        const text = chunk.toString();
        stderr += text;
        process.stderr.write(text);
      });

      child.on("error", reject);

      child.on("close", async (code) => {
        try {
          const payloadText = stdout.trim();
          if (!payloadText) {
            throw new Error("COD monitor produced no JSON payload on stdout");
          }

          const payload = JSON.parse(payloadText);
          const processed = await processCodMonitorResults(payload);

          if (code !== 0) {
            console.warn(
              `⚠ COD monitor exited with code ${code}, but payload was processed successfully.`,
            );
          }

          resolve({
            exitCode: code,
            processed,
            stderr,
          });
        } catch (err) {
          reject(
            new Error(
              `COD monitor execution failed: ${err.message}${stderr ? ` | stderr: ${stderr.trim()}` : ""}`,
            ),
          );
        }
      });
    });

    console.log(
      `✅ Scheduled COD monitor run processed | alerts=${result.processed.totalAlerts} | errors=${result.processed.totalErrors} | emails=${result.processed.emailDelivery?.sent || 0}`,
    );
    return result;
  } finally {
    codMonitorRunning = false;
    await releaseSchedulerLease();
  }
}

function scheduleNextRun() {
  const config = getSchedulerConfig();
  if (!config.enabled) return;

  if (codMonitorTimer) {
    clearTimeout(codMonitorTimer);
  }

  const delayMs = getDelayUntilNextRun(config.minute, SCHEDULE_TIME_ZONE);
  const nextRunLabel = formatScheduledTime(delayMs, SCHEDULE_TIME_ZONE);
  console.log(
    `🕒 Next COD monitor run scheduled for ${nextRunLabel} (${SCHEDULE_TIME_ZONE})`,
  );

  codMonitorTimer = setTimeout(async () => {
    try {
      await runCodMonitorJob();
    } catch (err) {
      console.error("🔥 Scheduled COD monitor run failed:", err.message);
    } finally {
      scheduleNextRun();
    }
  }, delayMs);
}

function startCodMonitorScheduler() {
  const config = getSchedulerConfig();
  if (!config.enabled) {
    console.log("🛑 COD monitor scheduler disabled.");
    return;
  }

  console.log(
    `🕐 COD monitor scheduler enabled to run every hour at minute ${String(config.minute).padStart(2, "0")} ${SCHEDULE_TIME_ZONE}`,
  );

  if (config.runOnBoot) {
    runCodMonitorJob().catch((err) => {
      console.error("🔥 Initial COD monitor run failed:", err.message);
    });
  }

  scheduleNextRun();
}

module.exports = {
  startCodMonitorScheduler,
  runCodMonitorJob,
};
