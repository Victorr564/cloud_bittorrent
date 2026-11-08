package com.microservice.bittorentservice.kafka.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microservice.bittorentservice.business.peer.PeerAddress;
import com.microservice.bittorentservice.model.TorrentData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class KafkaConsumerComponent{
    private static KafkaConsumerComponent kafkaService = null;

    private final Properties props = new Properties();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final KafkaConsumer<String, String> consumer;
    private volatile boolean running = true;
    private static final String TORRENT_TOPIC = "torrent.parsed";
    private static final String PEERS_TOPIC = "peers.updated";
    private KafkaConsumerComponent() {
        props.put(ConsumerConfig.GROUP_ID_CONFIG, System.getenv().getOrDefault("KAFKA_GROUP_ID","bittorent-service"));
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS","localhost:9092"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // we commit after handling
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.consumer = new KafkaConsumer<>(props);
    }

    public static KafkaConsumerComponent createService() {
        if (kafkaService == null) {
            kafkaService = new KafkaConsumerComponent(); // actually stored this time
        }
        return kafkaService;
    }

    public void consume(BiConsumer<String, TorrentData> torrentHandler, Consumer<List<PeerAddress>> peersHandler) {
        consumer.subscribe(List.of(TORRENT_TOPIC, PEERS_TOPIC));
        try {
            while (running) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    try {
                        switch (record.topic()) {
                            case TORRENT_TOPIC -> {
                                TorrentData data = objectMapper.readValue(record.value(), TorrentData.class);
                                torrentHandler.accept(record.key(), data);
                            }
                            case PEERS_TOPIC -> {
                                List<PeerAddress> peers = parsePeers(record.value());
                                if (!peers.isEmpty()) peersHandler.accept(peers);
                            }
                            default -> System.err.println("Unexpected topic: " + record.topic());
                        }
                    } catch (Exception e) {
                        // a bad message must not kill the loop or drop the rest of the batch
                        System.err.println("Skipping record at offset " + record.offset() + ": " + e.getMessage());
                    }
                }
                consumer.commitSync(); // after the whole batch was handled
            }
        } catch (WakeupException e) {
            // shutdown requested
        } finally {
            consumer.close();
        }
    }
    private List<PeerAddress> parsePeers(String json) throws JsonProcessingException {
        Map<String, Integer> raw = objectMapper.readValue(json, new TypeReference<Map<String, Integer>>() {});
        List<PeerAddress> peers = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : raw.entrySet()) {
            Integer port = entry.getValue();
            if (port != null && port > 0 && port <= 65535) {
                peers.add(new PeerAddress(entry.getKey(), port));
            }
        }
        return peers;
    }

}