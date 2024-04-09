const fs = require("fs");
const Kafka = require("node-rdkafka");

function readConfig(fileName) {
  const data = fs.readFileSync(fileName, "utf8").toString().split("\n");
  return data.reduce((config, line) => {
    const [key, value] = line.split("=");
    if (key && value) {
      config[key] = value;
    }
    return config;
  }, {});
}

function main() {
  const config = readConfig("client.properties");
  const topic = "test";
  const key = "key";
  const value = "value";

  // creates a new producer instance
  const producer = new Kafka.Producer(config);
  producer.connect();
  producer.on("ready", () => {
    // produces a sample message
    producer.produce(topic, -1, Buffer.from(value), Buffer.from(key));
    console.log(
      `Produced message to topic ${topic}: key = ${key} value = ${value}`
    );
  });

    // set the consumer's group ID, offset and initialize it
    config["group.id"] = "nodejs-group-1";
    const topicConfig = { "auto.offset.reset": "earliest" };
    const consumer = new Kafka.KafkaConsumer(config, topicConfig);
    consumer.connect();
  
    consumer
      .on("ready", () => {
        // subscribe to the topic and start polling for messages
        consumer.subscribe([topic]);
        consumer.consume();
      })
      .on("data", (message) => {
        // print incoming messages
        console.log(
          `Consumed message from topic ${
            message.topic
          }: key = ${message.key.toString()} value = ${message.key.toString()}`
        );
      });
}

main();