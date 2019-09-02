package com.github.johnschoeman.kafka_twitter;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class TwitterConsumer {
    public static void main(String[] args) throws IOException {
        new TwitterConsumer().run();
    }

    private TwitterConsumer() {}

    private void run() throws IOException {
        org.slf4j.Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

        // Kafka Consumer
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "elastic-search-application";
        String topic = "twitter-feed";

        KafkaConsumer<String, String> consumer = createKafkaConsumer(bootstrapServers, groupId, topic);

        // Elastic Search Client
        RestHighLevelClient client = createElasticSearchClient();
        ActionListener<IndexResponse> listener = createActionListener();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            logger.info("shutting down elastic search client...");
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            logger.info("Received " + records.count() + " records");

            for(ConsumerRecord<String, String> record : records) {
                // 2 strategies for idempotent ids:
                // kafka generic id
                // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                // twitter feed id
                String id = extractIdFromTweet(record.value());

                IndexRequest indexRequest = new IndexRequest("tweets")
                        .id(id)
                        .source(record.value(), XContentType.JSON);

                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                logger.info(indexResponse.getId());
                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            logger.info("Committing offsets...");
            consumer.commitSync();
            logger.info("Offsets have been committed");
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public KafkaConsumer<String, String> createKafkaConsumer(String bootstrapServers, String groupId, String topic) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }


    public RestHighLevelClient createElasticSearchClient() {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));
        return client;
    }

    private static JsonParser jsonParser = new JsonParser();
    private static String extractIdFromTweet(String tweetJson) {
        // gson library
        try {
            return jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get("id_str")
                    .getAsString();
        } catch (NullPointerException e) {
            return "2";
        }
    }
}
