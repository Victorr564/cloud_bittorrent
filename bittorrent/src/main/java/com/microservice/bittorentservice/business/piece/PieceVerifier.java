package com.microservice.bittorentservice.business.piece;

import com.microservice.bittorentservice.Sha1Hasher;
import com.microservice.bittorentservice.model.TorrentInfo;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class PieceVerifier {
    private final TorrentInfo info;

    public PieceVerifier(TorrentInfo info) {
        this.info = info;
    }

    public boolean verifyPiece(byte[] pieceBytes, int pieceIndex) throws NoSuchAlgorithmException {
        if (pieceIndex < 0 || pieceIndex >= info.numPieces()) return false;
        if (pieceBytes.length != info.pieceLengthAt(pieceIndex)) return false;
        return Arrays.equals(Sha1Hasher.sha1(pieceBytes), info.pieceHashes()[pieceIndex]);
    }
}