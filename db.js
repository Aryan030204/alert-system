const mysql = require("mysql2/promise");
require("dotenv").config();

const pool = mysql.createPool({
  host: process.env.MASTER_DB_HOST,
  user: process.env.MASTER_DB_USER,
  password: process.env.MASTER_DB_PASS,
  database: process.env.MASTER_DB_NAME,
});

(async () => {
  try {
    const [db] = await pool.query("SELECT DATABASE() AS db");
    console.log("📌 Connected to DB:", db[0].db);
  } catch (err) {
    console.error("❌ DB connection error:", err);
  }
})();

module.exports = pool;
