package com.github.rohan.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);


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
            String topic = "first_topic";
            String value = "hello World"+Integer.toString(i);
            String key = "id_"+Integer.toString(i);
            logger.info("key"+key);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic,key,value);
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Topic " + recordMetadata.topic());
                        logger.info("Partitions " + recordMetadata.partition());
                        logger.info("Offset " + recordMetadata.offset() + " Timestamp " + recordMetadata.timestamp());
                    }
                    else{
                        logger.error("Error in producing"+e);
                    }
                }
            }).get();
        }
            producer.flush();
            producer.close();

    }
}
