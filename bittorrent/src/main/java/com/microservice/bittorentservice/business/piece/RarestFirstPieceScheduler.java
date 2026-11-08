package com.microservice.bittorentservice.business.piece;

import com.microservice.bittorentservice.business.peer.PeerConnection;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class RarestFirstPieceScheduler implements IPieceScheduler{
    @Override
    public Optional<PieceTarget> schedulePieces(boolean[] have, List<PeerConnection> peers, PieceTracker pieceTracker) {
        Map<Integer, List<PeerConnection>> holders = new HashMap<>();

        for (PeerConnection peer : peers) {
            boolean[] peerPieces = peer.getPeerPieces();

            int n = Math.min(peerPieces.length, have.length);
            for (int i = 0; i < n; i++) {
                if (peerPieces[i] && !have[i] && !pieceTracker.isInProgress(i)) {
                    holders.computeIfAbsent(i, k -> new ArrayList<>()).add(peer);
                }
            }
        }
        if (holders.isEmpty()) return Optional.empty(); // nothing needed that anyone has

        // pass 1: smallest peer count
        int min = holders.values().stream().mapToInt(List::size).min().getAsInt();

        // pass 2: all pieces tied at that count
        List<Integer> rarest = holders.entrySet().stream()
                .filter(e -> e.getValue().size() == min)
                .map(Map.Entry::getKey)
                .toList();

        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        int piece = rarest.get(rnd.nextInt(rarest.size()));

        List<PeerConnection> candidates = holders.get(piece);
        PeerConnection peer = candidates.get(rnd.nextInt(candidates.size()));

        return Optional.of(new PieceTarget(piece, peer));
    }
}
