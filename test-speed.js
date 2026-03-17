require("dotenv").config({ path: "./.env" });
const { processIncomingEvent } = require("./alertEngine.js");

async function runSpeedTest() {
  console.log("🧪 Starting Speed Alert Test...\n");

  // Mock Event Payload
  // Note: Adjust the brand_id and brand name to match a brand in your system 
  // that has a "performance" metric alert rule configured.
  const testEvent = {
    "brand_id": 4,             // Replace with accurate brand_id if known
    "brand": "NEULIFE",         // Replace with TMC, NEULIFE, etc.
    "performance": 30,         // Low value to trigger condition drops Lookback Avg
    "hour": 15,                // Peak hour example
    "total_sales": 0,          // Dummy values to bypass calculation locks
    "total_orders": 0,
    "total_sessions": 0,
    "total_atc_sessions": 0
  };

  try {
    console.log("📤 Processing test event for Speed Alert Analytics:");
    console.log(JSON.stringify(testEvent, null, 2));
    
    console.log("\n--- Alert Engine Output ---");
    await processIncomingEvent(testEvent);
    console.log("----------------------------");
    
    console.log("\n✅ Test completion signal reached!");
    console.log("👉 Check console logs above to verify if aggregate metrics were pulled successfully.");
    console.log("👉 If fully satisfied, an email triggers if state changes.");

  } catch (err) {
    console.error("\n❌ Test execution failed:", err.message);
    console.error(err.stack);
  } finally {
    // Force exit if any connections are left open (e.g., MySQL pool or Mongo clients)
    setTimeout(() => process.exit(0), 1000);
  }
}

runSpeedTest();
