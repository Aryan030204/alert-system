const { Redis } = require("@upstash/redis");

const redis = new Redis({
  url: "https://resolved-goat-18924.upstash.io",
  token: "AUnsAAIncDIzNmNiOTFjZmZiNGM0OTkwYTkwZDcwMDBhMWQ2ZDMzNnAyMTg5MjQ",
});

async function test() {
  const eventAOV = {
    brand_id: 1,
    brand: "PTS",
    total_sales: 6000,
    total_orders: 10,
    aov: 600,
    total_sessions: 5000,
    total_atc_sessions: 200,
    gross_sales: 80000,
  };

  const eventCRA = {
    brand_id: 4,
    brand: "TMC",
    total_sales: 15000,
    total_orders: 5,
    aov: 3000,
    total_sessions: 20000,
    total_atc_sessions: 1200,
    gross_sales: 100000,
  };

  const eventATC = {
    brand_id: 4,
    brand: "TMC",
    total_sales: 20000,
    total_orders: 25,
    aov: 2000,
    total_sessions: 1000, // ✔ Keeps CR high (5%)
    total_atc_sessions: 50, // 🔥 ATC drop alert triggers
    gross_sales: 1, // Prevents gross sales alert
  };

  const eventGSA = {
    brand_id: 4,
    brand: "TMC",
    total_sales: 2000,
    total_orders: 10,
    aov: 200,
    total_sessions: 8000,
    total_atc_sessions: 500,
  };

  const eventLOW = {
    brand_id: 4,
    brand: "TMC",
    total_sales: 2000,
    total_orders: 2,
    total_sessions: 20,
    aov: 200,
    total_atc_sessions: 100,
  };

  await redis.rpush("summary_updates_queue", JSON.stringify(eventATC));
  console.log("Pushed!");
}

test();
