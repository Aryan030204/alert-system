const os = require("os");
const path = require("path");
const { spawn } = require("child_process");
const { MongoClient } = require("mongodb");
const { processDiscountMonitorResults } = require("./alertEngine");

const LOCK_NAME = "discount_monitor_interval";
const schedulerOwner = `${os.hostname()}:${process.pid}`;

let discountMonitorTimer = null;
let discountMonitorRunning = false;
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
    enabled: parseBoolean(process.env.DISCOUNT_MONITOR_SCHEDULE_ENABLED, false),
    runOnBoot: parseBoolean(process.env.DISCOUNT_MONITOR_RUN_ON_BOOT, false),
    dryRun: parseBoolean(process.env.DISCOUNT_MONITOR_SCHEDULE_DRY_RUN, false),
    intervalMinutes: Number(process.env.DISCOUNT_MONITOR_INTERVAL_MINUTES || 15),
    pythonBin: process.env.DISCOUNT_MONITOR_PYTHON_BIN || "python",
    scriptPath:
      process.env.DISCOUNT_MONITOR_SCRIPT_PATH ||
      path.join(__dirname, "discount_monitor", "run.py"),
    brands: parseBrands(process.env.DISCOUNT_MONITOR_BRANDS),
    leaseMinutes: Number(process.env.DISCOUNT_MONITOR_SCHEDULE_LEASE_MINUTES || 10),
  };
}

async function getSchedulerLockCollection() {
  if (schedulerMongoClient) {
    return schedulerMongoClient.db().collection("scheduler_locks");
  }

  if (!process.env.MONGO_URI) {
    throw new Error("MONGO_URI is required for discount monitor scheduler locking");
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
    console.warn("⚠️ Failed to release discount monitor scheduler lease:", err.message);
  }
}

async function runDiscountMonitorJob() {
  if (discountMonitorRunning) {
    console.log("⏭ Discount monitor scheduler skipped: previous run still in progress.");
    return null;
  }

  const config = getSchedulerConfig();
  const leaseAcquired = await acquireSchedulerLease(config.leaseMinutes);
  if (!leaseAcquired) {
    console.log("⏭ Discount monitor scheduler skipped: another instance owns the run lease.");
    return null;
  }

  const args = [config.scriptPath, "--json-output"];
  if (config.dryRun) args.push("--dry-run");
  if (config.brands.length) {
    args.push("--brands", ...config.brands);
  }

  discountMonitorRunning = true;
  console.log(
    `🏷 Starting scheduled discount monitor run with ${config.pythonBin} ${args.join(" ")}`,
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
            throw new Error("Discount monitor produced no JSON payload on stdout");
          }

          const payload = JSON.parse(payloadText);
          const processed = await processDiscountMonitorResults(payload);

          if (code !== 0) {
            console.warn(
              `⚠ Discount monitor exited with code ${code}, but payload was processed successfully.`,
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
              `Discount monitor execution failed: ${err.message}${stderr ? ` | stderr: ${stderr.trim()}` : ""}`,
            ),
          );
        }
      });
    });

    console.log(
      `✅ Scheduled discount monitor run processed | flagged=${result.processed.flaggedCount} | alerts=${result.processed.totalAlerts} | emails=${result.processed.emailDelivery?.sent || 0}`,
    );
    return result;
  } finally {
    discountMonitorRunning = false;
    await releaseSchedulerLease();
  }
}

function scheduleNextRun() {
  const config = getSchedulerConfig();
  if (!config.enabled) return;

  if (discountMonitorTimer) {
    clearTimeout(discountMonitorTimer);
  }

  const delayMs = Math.max(1, config.intervalMinutes) * 60 * 1000;
  console.log(
    `🕒 Next discount monitor run in ${config.intervalMinutes} minute(s).`,
  );

  discountMonitorTimer = setTimeout(async () => {
    try {
      await runDiscountMonitorJob();
    } catch (err) {
      console.error("🔥 Scheduled discount monitor run failed:", err.message);
    } finally {
      scheduleNextRun();
    }
  }, delayMs);
}

function startDiscountMonitorScheduler() {
  const config = getSchedulerConfig();
  if (!config.enabled) {
    console.log("🛑 Discount monitor scheduler disabled.");
    return;
  }

  console.log(
    `🕐 Discount monitor scheduler enabled to run every ${config.intervalMinutes} minute(s).`,
  );

  if (config.runOnBoot) {
    runDiscountMonitorJob().catch((err) => {
      console.error("🔥 Initial discount monitor run failed:", err.message);
    });
  }

  scheduleNextRun();
}

module.exports = {
  startDiscountMonitorScheduler,
  runDiscountMonitorJob,
};
