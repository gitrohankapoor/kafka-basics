package com.github.rohan.kafka.twitter;


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

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    public static void main(String[] args) {

        new TwitterProducer().run();

    }
    public void run(){
        //create twitter client
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100);
        Client client = createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        client.connect();
        KafkaProducer<String,String> producer = createKafkaProducer();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down");
            client.stop();
            producer.close();
        }));
        while (!client.isDone()) {
            String msg = null;
            try {
                 msg = msgQueue.poll(5, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg!=null){
                logger.info("tweets "+msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e!=null){
                            logger.error("Something bad happened");
                        }
                    }
                });
            }

        }

        //create kafka producer

        //loop to send tweets to kafka

    }
    public Client createTwitterClient(BlockingQueue<String> msgQueue){
        String consumerKey = "DudMJ63sW9DhFwq1F5uPIrxmS";
        String consumerSecret="YBuC9lw8ccjmb37Sati2RL0PlF5PTMCLEftzYjm586lSqOttwM";
        String token = "253190569-GurTdhmGDqIUSOVWLoveRaNJtox6ip0WgO1vOYop";
        String secret = "ATlRDFUioSMmAzXFc3kjZHI23rphZxnyodHf2uFReHHbW";


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();


        List<String> terms = Lists.newArrayList("mancity");
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;


    }
    public KafkaProducer<String,String> createKafkaProducer(){
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Create a producer Class
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }
}
