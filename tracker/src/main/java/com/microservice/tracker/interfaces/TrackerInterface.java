package com.microservice.tracker.interfaces;

import com.microservice.tracker.models.TorrentData;

import java.io.IOException;

public interface TrackerInterface {
    void announce(TorrentData torrentData) throws IOException, InterruptedException;
    void forwardPeers();
}
