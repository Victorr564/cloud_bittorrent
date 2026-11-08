package com.microservice.bittorentservice.business.piece;

import com.microservice.bittorentservice.business.peer.PeerConnection;
//scheduling
public record PieceTarget(int pieceIndex, PeerConnection peer) {}