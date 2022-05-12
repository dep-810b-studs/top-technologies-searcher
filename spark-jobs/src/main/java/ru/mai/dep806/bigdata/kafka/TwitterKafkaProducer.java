package ru.mai.dep806.bigdata.kafka;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TwitterKafkaProducer {
    private static final String topic = "test1";

    private static void run(String consumerKey, String consumerSecret,
                            String token, String secret) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "172.16.82.107:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(properties);

        BlockingQueue<String> queue = new LinkedBlockingQueue<>(10000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        // add some track terms
        endpoint.trackTerms(Lists.newArrayList("java"));

        Authentication auth = new OAuth1(consumerKey, consumerSecret, token,
                secret);

        // Create a new BasicClient. By default gzip is enabled.
        Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
                .endpoint(endpoint).authentication(auth)
                .processor(new StringDelimitedProcessor(queue)).build();

        // Establish a connection
        client.connect();

        // Do whatever needs to be done with messages
        for (int msgRead = 0; msgRead < 1000; msgRead++) {
            ProducerRecord<String, String> message = null;
            try {
                message = new ProducerRecord<>(topic, queue.take());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            producer.send(message);
        }
        producer.close();
        client.stop();

    }

    public static void main(String[] args) {
        TwitterKafkaProducer.run(args[0], args[1], args[2], args[3]);
    }
}
