package com.microservice.tracker.models;

import java.util.List;
import java.util.Map;

public record TorrentData(
        String announceURL,
        byte[] infoHash,
        Long length,
        String name,
        Long pieceLength,
        byte[] pieces,
        List<Map<String,Object>> files
){}