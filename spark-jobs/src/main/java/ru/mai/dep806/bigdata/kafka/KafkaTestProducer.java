package ru.mai.dep806.bigdata.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaTestProducer {
    private static final String topic = "test1";

    public static void run() throws InterruptedException {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "172.16.82.107:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(properties);

        for (int msgRead = 0; msgRead < 10; msgRead++) {
            ProducerRecord<String, String> message = null;
            message = new ProducerRecord<>(topic, "message " + msgRead);
            producer.send(message);
        }
        producer.close();
    }

    public static void main(String[] args) {
        try {
            KafkaTestProducer.run();
        } catch (InterruptedException e) {
            System.out.println(e);
        }
    }
}
