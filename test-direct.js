require("dotenv").config({ path: "./.env" });
const { processIncomingEvent } = require("./alertEngine.js");

async function testDirect() {
  console.log("🧪 Testing Alert Engine Directly...\n");

  const testEvent = {
    brand_id: 1,
    brand: "PTS",
    total_sales: 6000,
    total_orders: 10,
    aov: 600,
    total_sessions: 5000,
    total_atc_sessions: 200,
    gross_sales: 80000,
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


