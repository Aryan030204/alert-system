let amqpLib;

class RabbitMQPublisher {
  constructor(options = {}) {
    this.url = options.url || process.env.RABBITMQ_URL;
    this.exchange = options.exchange || process.env.RABBITMQ_EXCHANGE || "alerts.events";
    const publishDisabledValue =
      options.publishDisabled ?? process.env.RABBITMQ_PUBLISH_DISABLED ?? "";
    this.publishDisabled =
      String(publishDisabledValue).toLowerCase() === "true";
    this.connection = null;
    this.channel = null;
    this.connectPromise = null;
  }

  async ensureChannel() {
    if (this.publishDisabled) return null;
    if (this.channel) return this.channel;
    if (this.connectPromise) {
      await this.connectPromise;
      return this.channel;
    }

    if (!this.url) {
      throw new Error("RABBITMQ_URL is required unless RABBITMQ_PUBLISH_DISABLED=true");
    }

    this.connectPromise = (async () => {
      if (!amqpLib) {
        try {
          amqpLib = require("amqplib");
        } catch (err) {
          throw new Error(`amqplib dependency is required for RabbitMQ publishing: ${err.message}`);
        }
      }

      this.connection = await amqpLib.connect(this.url);
      this.connection.on("error", (err) => {
        console.error("RabbitMQ connection error:", err.message);
      });
      this.connection.on("close", () => {
        this.connection = null;
        this.channel = null;
        this.connectPromise = null;
        console.warn("RabbitMQ connection closed");
      });

      this.channel = await this.connection.createChannel();
      this.channel.on("error", (err) => {
        console.error("RabbitMQ channel error:", err.message);
      });
      await this.channel.assertExchange(this.exchange, "topic", { durable: true });
      console.log(`✅ RabbitMQ publisher ready (exchange=${this.exchange})`);
    })();

    try {
      await this.connectPromise;
      return this.channel;
    } catch (err) {
      this.connectPromise = null;
      throw err;
    }
  }

  async publishAlertFiredEvent(envelope) {
    if (!envelope || envelope.eventType !== "alert.fired") {
      throw new Error("publishAlertFiredEvent expects an alert.fired envelope");
    }

    if (this.publishDisabled) {
      console.log("[RabbitMQ dry-run] alert.fired publish skipped", {
        exchange: this.exchange,
        routingKey: "alerts.fired",
        envelope,
      });
      return { published: false, dryRun: true };
    }

    const channel = await this.ensureChannel();
    const payloadBuffer = Buffer.from(JSON.stringify(envelope));
    const published = channel.publish(this.exchange, "alerts.fired", payloadBuffer, {
      persistent: true,
      contentType: "application/json",
      messageId: envelope.eventId,
      type: envelope.eventType,
      timestamp: Date.now(),
      headers: {
        idempotencyKey: envelope.idempotencyKey,
        tenantId: envelope.tenantId,
        brandId: envelope.brandId,
        alertId: envelope.alertId,
        schemaVersion: envelope.schemaVersion,
      },
    });

    if (!published) {
      // We do not silently drop broker backpressure; caller can retry the processing unit.
      throw new Error("RabbitMQ channel.publish returned false (backpressure / buffer full)");
    }

    return { published: true };
  }

  async close() {
    const channel = this.channel;
    const connection = this.connection;
    this.channel = null;
    this.connection = null;
    this.connectPromise = null;

    if (channel) {
      try {
        await channel.close();
      } catch (err) {
        console.warn("RabbitMQ channel close error:", err.message);
      }
    }

    if (connection) {
      try {
        await connection.close();
      } catch (err) {
        console.warn("RabbitMQ connection close error:", err.message);
      }
    }
  }
}

const rabbitmqPublisher = new RabbitMQPublisher();

module.exports = {
  RabbitMQPublisher,
  rabbitmqPublisher,
};
