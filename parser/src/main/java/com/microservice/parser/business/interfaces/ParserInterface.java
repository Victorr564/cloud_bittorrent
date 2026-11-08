package com.microservice.parser.business.interfaces;

import com.microservice.parser.business.models.TorrentData;

public interface ParserInterface {
    TorrentData parseTorrent(byte[] file);
}
