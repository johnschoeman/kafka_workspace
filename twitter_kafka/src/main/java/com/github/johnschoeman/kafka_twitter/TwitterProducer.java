package com.github.johnschoeman.kafka_twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterProducer {
    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    private TwitterProducer() {}

    private void run() {
        org.slf4j.Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "twitter-feed";
        KafkaProducer<String, String> producer = createKafkaProducer(bootstrapServers);

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        LinkedBlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        List<String> terms = Lists.newArrayList("scala", "kafka");
//        List<String> terms = Lists.newArrayList("twitter", "api");
        Client client = createTwitterClient(msgQueue, terms);

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            logger.info("shutting down client from twitter...");
            client.stop();
            logger.info("closing produer...");
            producer.flush();
            producer.close();
            logger.info("Application has exited");
        }));

        client.connect();
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, msg);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        logger.error("Exception raised when writing record: ", e);
                    }
                }
            });
        }
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue, List<String> terms){
        String appConfig = "app.config";
        String consumerKey = null;
        String consumerSecret = null;
        String token = null;
        String tokenSecret = null;

        try {
            InputStream input = new FileInputStream(appConfig);
            Properties twitterClientProperties = new Properties();
            twitterClientProperties.load(input);
            consumerKey = twitterClientProperties.getProperty("consumerKey");
            consumerSecret = twitterClientProperties.getProperty("consumerSecret");
            token = twitterClientProperties.getProperty("token");
            tokenSecret = twitterClientProperties.getProperty("tokenSecret");
        } catch (IOException e) {
            e.printStackTrace();
        }

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }

    public KafkaProducer<String, String> createKafkaProducer(String bootstrapServers){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // high throughput producer (at the expense of a bit of latency and cpu usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }
}
