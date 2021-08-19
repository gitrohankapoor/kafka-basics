package com.github.rohan.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoCallbacks {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemoCallbacks.class);


            //create producer properties
            Properties properties = new Properties();
            String bootstrapServers = "127.0.0.1:9092";
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            //Create a producer Class
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0;i<10;i++) {
            System.out.println("Hello World!" + i);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first_topic", "hello world" + Integer.toString(i));
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Topic " + recordMetadata.topic());
                        logger.info("Partitions " + recordMetadata.partition());
                        logger.info("Offset " + recordMetadata.offset() + " Timestamp " + recordMetadata.timestamp());
                    }
                }
            });
        }
            producer.flush();
            producer.close();

    }
}
