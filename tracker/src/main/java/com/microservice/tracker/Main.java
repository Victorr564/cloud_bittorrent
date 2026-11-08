package com.microservice.tracker;
import com.microservice.tracker.interfaces.TrackerInterface;
import com.microservice.tracker.models.TorrentData;
import com.microservice.tracker.services.TrackerService;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.*;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {


        TrackerInterface trackerInterface=new TrackerService();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS","localhost:9092"));
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, System.getenv().getOrDefault("KAKFA_GROUP_ID","tracker-service"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        ObjectMapper objectMapper=new ObjectMapper();
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        Consumer<String,String> consumer=null;
        try
        {
            consumer=new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList("torrent.parsed"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                        TorrentData torrentData=objectMapper.readValue(record.value(), TorrentData.class);
                        trackerInterface.announce(torrentData);
                        trackerInterface.forwardPeers();
                }
            }

        }
        catch(Exception e)
        {
            System.err.println("Error creating or sending producer: " + e.getMessage());
            e.printStackTrace();
        }
        finally
        {

            if (consumer != null) {
                try {
                    consumer.close();
                } catch (Exception e) {
                    System.err.println("Error closing producer: " + e.getMessage());
                    e.printStackTrace();
                }
            }

        }
        }
}