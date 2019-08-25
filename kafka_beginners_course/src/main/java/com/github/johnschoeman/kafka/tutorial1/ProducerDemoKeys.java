package com.github.johnschoeman.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServers = "127.0.0.1:9092";

        // create producer properties

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i=0; i < 10; i++) {
            // create a producer record

            String topic =  "first_topic";
            String value = "hello world " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            // final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, value);

            logger.info("Key: " + key);

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes everytime a record is successfully sent or an exception is thrown.

                    if (e == null) {
                        // the record was successfully sent.
                        logger.info("Received new metadata: \n" +
                                "topic: " + recordMetadata.topic() + "\n" +
                                "partition: " + recordMetadata.partition() + "\n" +
                                "offset: " + recordMetadata.offset() + "\n" +
                                "timestamp: " + recordMetadata.timestamp() + "\n"
                        );
                    } else {
                        logger.error("error while producing: ", e);
                    }
                }
            }).get(); // make synchronous -- don't do this in production
        }

        // flush data

        producer.flush();

        // flush and close producer

        producer.close();
    }
}
