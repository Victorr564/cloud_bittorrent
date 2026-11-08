package com.microservice.bittorentservice.business.piece;

import com.microservice.bittorentservice.business.peer.PeerConnection;

import java.util.List;
import java.util.Optional;

public interface IPieceScheduler {
    Optional<PieceTarget> schedulePieces(boolean[] have, List<PeerConnection> peers,PieceTracker pieceTracker);
}
