require("dotenv").config();
const pool = require("./db");

(async () => {
  const [rows] = await pool.query("SELECT * FROM alerts WHERE brand_id = 1");
  console.log(rows);
})();
