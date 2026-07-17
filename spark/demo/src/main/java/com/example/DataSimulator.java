package com.example;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class DataSimulator {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            String topic = "energy-measurement";
        for (int i = 0; i < 10; i++) {
            String type = (i % 2 == 0) ? "producer" : "consumer";
            double value = (Math.random() * 50) + 10;

            String event = String.format(
                "{\"nodeId\": \"node_%d\", \"type\": \"%s\", \"districtId\": \"district_A\", \"value\": %.2f, \"timestamp\": \"%s\"}",
                i, type, value, java.time.Instant.now()
            );

            producer.send(new ProducerRecord<>(topic, "district_A", event));
            System.out.println("Pushed to Kafka: " + event);

            Thread.sleep(2000);
        }
        }
    }

}
