package com.microservice.bittorentservice.kafka.controllers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.microservice.bittorentservice.business.peer.PeerAddress;
import com.microservice.bittorentservice.kafka.config.KafkaConsumerComponent;
import com.microservice.bittorentservice.model.TorrentData;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class KafkaController {
    private final KafkaConsumerComponent kafkaConsumerComponent;

    public KafkaController(
                           KafkaConsumerComponent kafkaConsumerComponent) {
        this.kafkaConsumerComponent = kafkaConsumerComponent;
    }

    public void sendTorrentMetadata(String torrentPath, String topic, String key) throws JsonProcessingException {

    }

    /**
     * Replaces receiveTorrentMetadata(). Nothing "fetches" a torrent anymore: every record
     * from torrent.parsed is pushed to the handler as (key, TorrentData).
     * This BLOCKS the calling thread, so run it on its own thread.
     */
    public void listen(BiConsumer<String, TorrentData> onTorrent, Consumer<List<PeerAddress>> onPeers) {
        kafkaConsumerComponent.consume(onTorrent, onPeers);
    }
}