package com.microservice.bittorentservice.model;

import java.util.List;
import java.util.Map;

public record TorrentData(
        String announceURL,
        byte[] infoHash,
        Long length, //single file case
        String name,
        Long pieceLength,
        byte[] pieces,
        List<Map<String,Object>> files //multi-file case
) {}