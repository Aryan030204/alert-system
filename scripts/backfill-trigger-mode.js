require("dotenv").config({ path: "./.env" });
const { MongoClient } = require("mongodb");

async function main() {
  if (!process.env.MONGO_URI) {
    throw new Error("MONGO_URI not set");
  }

  const client = new MongoClient(process.env.MONGO_URI);
  await client.connect();

  try {
    const db = client.db();
    const alerts = db.collection("alerts");

    const result = await alerts.updateMany(
      {
        $or: [
          { trigger_mode: { $exists: false } },
          { is_dsl_engine_alert: { $exists: false } },
        ],
      },
      {
        $set: {
          trigger_mode: "alert_system",
          is_dsl_engine_alert: false,
        },
      }
    );

    console.log("Backfill completed:", {
      matchedCount: result.matchedCount,
      modifiedCount: result.modifiedCount,
      defaultValues: {
        trigger_mode: "alert_system",
        is_dsl_engine_alert: false,
      },
    });
  } finally {
    await client.close();
  }
}

main().catch((err) => {
  console.error("Backfill failed:", err.message);
  process.exit(1);
});
