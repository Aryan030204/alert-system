const axios = require("axios");

async function sendSlackAlert(config, message) {
  try {
    await axios.post(config.webhook_url, { text: message });
  } catch (err) {
    console.error("Slack error:", err);
  }
}

module.exports = { sendSlackAlert };
