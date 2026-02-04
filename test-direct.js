require("dotenv").config({ path: "./.env" });
const { processIncomingEvent } = require("./alertEngine.js");

async function testDirect() {
  console.log("🧪 Testing Alert Engine Directly...\n");

  const testEvent = {
    "brand_id": 3,
    "brand": "TMC",
    "performance": 48,
    "total_sales": 100,
    "total_orders": 10,
    "aov": 10,
    "total_sessions": 500,
    "total_atc_sessions": 50,
    "gross_sales": 100
  };

  try {
    console.log("📤 Sending test event:", testEvent);
    await processIncomingEvent(testEvent);
    console.log("\n✅ Test completed!");
  } catch (err) {
    console.error("\n❌ Test failed:", err);
    console.error(err.stack);
  }
}

testDirect();


