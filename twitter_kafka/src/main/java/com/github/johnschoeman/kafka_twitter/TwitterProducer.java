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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
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

        String appConfig = "app.config";
        String bootstrapServers = "127.0.0.1:9092";
        String topic = "twitter-feed";
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

        logger.info("Creating producer thread");
        CountDownLatch latch = new CountDownLatch(1);

        final Runnable producerRunnable = new ProducerRunnable(
                bootstrapServers,
                topic,
                consumerKey,
                consumerSecret,
                token,
                tokenSecret,
                latch
        );

        Thread thread = new Thread(producerRunnable);

        thread.start();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ProducerRunnable) producerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e){
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted: ", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ProducerRunnable implements Runnable {
        private CountDownLatch latch;
        private String topic;
        private KafkaProducer<String, String> producer;
        private BlockingQueue<String> msgQueue;
        private ClientBuilder builder;
        private Client hosebirdClient;
        private Logger logger = LoggerFactory.getLogger(ProducerRunnable.class.getName());

        public ProducerRunnable(String bootstrapServers,
                                String topic,
                                String consumerKey,
                                String consumerSecret,
                                String token,
                                String tokenSecret,
                                CountDownLatch latch) {
            this.latch = latch;
            this.topic = topic;

            // Kafka Producer
            Properties properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            producer = new KafkaProducer<String, String>(properties);

            // Twitter Client
            /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
            msgQueue = new LinkedBlockingQueue<String>(100000);

            /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
            Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
            StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

            // Optional: set up some followings and track terms
            ArrayList<Long> followings = Lists.newArrayList(1234L, 566788L);
            List<String> terms = Lists.newArrayList("twitter", "api");
            hosebirdEndpoint.followings(followings);
            hosebirdEndpoint.trackTerms(terms);

            Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

            builder = new ClientBuilder()
                    .name("Hosebird-Client-01")                              // optional: mainly for the logs
                    .hosts(hosebirdHosts)
                    .authentication(hosebirdAuth)
                    .endpoint(hosebirdEndpoint)
                    .processor(new StringDelimitedProcessor(msgQueue));

            hosebirdClient = builder.build();
        }

        @Override
        public void run() {
            hosebirdClient.connect();
            try {
                while (!hosebirdClient.isDone()) {
                    String msg = null;
                    try {
                        msg = msgQueue.take();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, msg);
                    producer.send(record);
                }
            } catch (IllegalStateException e) {
                logger.info("Received shutdown signal.");
            } finally {
                logger.info("Closing twitter client.");
                hosebirdClient.stop();
                latch.countDown();
            }

        }

        public void shutdown() {
            producer.flush();
            producer.close();
        }
    }
}
