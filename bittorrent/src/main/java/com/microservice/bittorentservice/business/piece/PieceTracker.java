package com.microservice.bittorentservice.business.piece;

import com.microservice.bittorentservice.model.TorrentInfo;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PieceTracker {
    private final TorrentInfo info;
    // piece index -> indexes of the blocks that have arrived for that piece
    private final Map<Integer, Set<Integer>> trackingMap = new ConcurrentHashMap<>();

    public PieceTracker(TorrentInfo info) {
        this.info = info;
    }

    /** Atomically reserves a piece before requesting it. False if someone already did. */
    public boolean tryClaim(int pieceIndex) {
        return trackingMap.putIfAbsent(pieceIndex, ConcurrentHashMap.newKeySet()) == null;
    }

    /** Records an arrived block. Returns true when every block of the piece is in. */
    public boolean registerBlock(int pieceIndex, int blockIndex) {
        Set<Integer> blocks = trackingMap.get(pieceIndex);
        if (blocks == null) return false;                       // released or never claimed: ignore
        return blocks.add(blockIndex) && blocks.size() == info.blocksInPiece(pieceIndex);
    }

    public void release(int pieceIndex) {
        trackingMap.remove(pieceIndex);
    }

    public boolean isInProgress(int pieceIndex) {
        return trackingMap.containsKey(pieceIndex);
    }
}