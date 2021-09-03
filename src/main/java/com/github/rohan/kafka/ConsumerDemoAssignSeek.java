package com.github.rohan.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my_fourth_application";
        String topic = "first_topic";
        logger.info("ConsumerDemo");
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
        //consumer.subscribe(Arrays.asList(topic));
        //assign and seek are mostly used to replay data or fetch specific message
        TopicPartition topicPartitiontoReadFrom = new TopicPartition(topic,0);
        long offsetToReadFrom = 1L;
        consumer.assign(Arrays.asList(topicPartitiontoReadFrom));
        //seek
        consumer.seek(topicPartitiontoReadFrom,offsetToReadFrom);
        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar=0;
        while(keepOnReading){
            ConsumerRecords<String,String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord consumerRecord : consumerRecords){
                numberOfMessagesReadSoFar++;
                logger.info("key "+consumerRecord.key()+" Value "+consumerRecord.value());
                logger.info("Partition" + consumerRecord.partition()+ " offset"+consumerRecord.offset());
                if(numberOfMessagesReadSoFar>=numberOfMessagesToRead){
                    keepOnReading= false;
                    break;
                }
            }

        }
    }
}
