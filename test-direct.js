require("dotenv").config({ path: "./.env" });
const { processIncomingEvent } = require("./alertEngine.js");

async function testDirect() {
  console.log("🧪 Testing Alert Engine Directly...\n");

  const testEvent = {
  "brand_id": 3,
  "brand": "TMC",
  "total_sales": 505628.0185546875,
  "total_orders": 0,
  "aov": 1283.3198440474303,
  "total_sessions": 500,
  "total_atc_sessions": 0,
  "gross_sales": 505628.0185546875
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


