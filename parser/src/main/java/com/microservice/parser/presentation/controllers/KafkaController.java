package com.microservice.parser.presentation.controllers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.microservice.parser.business.interfaces.ParserInterface;
import com.microservice.parser.presentation.config.KafkaProducerComponent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class KafkaController {
    @Autowired
    private ParserInterface parserInterface;
    @Autowired
    private KafkaProducerComponent kafkaProducerComponent;

    public void sendTorrentMetadata(byte[] torrentFile,String topic,String key) throws JsonProcessingException {
        ObjectMapper objectMapper=new ObjectMapper();
        String message=objectMapper.writeValueAsString(parserInterface.parseTorrent(torrentFile));
        kafkaProducerComponent.sendRecord(topic,key,message);
    }

}
