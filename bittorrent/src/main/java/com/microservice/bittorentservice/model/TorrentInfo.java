
package com.microservice.bittorentservice.model;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public record TorrentInfo(
        byte[] infoHash,
        long fileLength,
        int pieceLength,
        int numPieces,
        int lastPieceLength,
        byte[][] pieceHashes,
        List<Map<String, Object>> files) {

    public static final int BLOCK_LENGTH = 16_384;
    private static final int HASH_SIZE = 20;

    public static TorrentInfo from(TorrentData d) {
        if (d == null
                || d.infoHash() == null
                || d.pieces() == null
                || d.length() == null
                || d.pieceLength() == null) {
            throw new IllegalArgumentException(
                    "torrent data is missing required fields");
        }

        byte[] pieces = d.pieces();
        long fileLength = d.length();
        int pieceLength = Math.toIntExact(d.pieceLength());

        if (fileLength < 0) {
            throw new IllegalArgumentException(
                    "file length cannot be negative");
        }

        if (pieceLength <= 0) {
            throw new IllegalArgumentException(
                    "piece length must be positive");
        }

        if (d.infoHash().length != HASH_SIZE) {
            throw new IllegalArgumentException(
                    "info hash must be 20 bytes");
        }

        if (pieces.length % HASH_SIZE != 0) {
            throw new IllegalArgumentException(
                    "pieces length is not a multiple of 20");
        }

        long expectedPieces =
                fileLength / pieceLength
                        + (fileLength % pieceLength == 0 ? 0 : 1);

        int numPieces = pieces.length / HASH_SIZE;

        if (expectedPieces != numPieces) {
            throw new IllegalArgumentException(
                    "piece count doesn't match file length");
        }

        int lastPieceLength;
        if (numPieces == 0) {
            lastPieceLength = 0;
        } else {
            long remainder = fileLength % pieceLength;
            lastPieceLength = remainder == 0
                    ? pieceLength
                    : (int) remainder;
        }

        byte[][] pieceHashes = new byte[numPieces][HASH_SIZE];

        for (int i = 0; i < numPieces; i++) {
            System.arraycopy(
                    pieces,
                    i * HASH_SIZE,
                    pieceHashes[i],
                    0,
                    HASH_SIZE);
        }

        return new TorrentInfo(
                d.infoHash(),
                fileLength,
                pieceLength,
                numPieces,
                lastPieceLength,
                pieceHashes,
                d.files());
    }

    /** Length of a piece; the last piece may be shorter. */
    public int pieceLengthAt(int pieceIndex) {
        checkPieceIndex(pieceIndex);

        return pieceIndex == numPieces - 1
                ? lastPieceLength
                : pieceLength;
    }

    /** Number of blocks in a piece. */
    public int blocksInPiece(int pieceIndex) {
        int length = pieceLengthAt(pieceIndex);

        return (length + BLOCK_LENGTH - 1) / BLOCK_LENGTH;
    }

    /** Length of a block within a piece. */
    public int blockLengthAt(int pieceIndex, int blockIndex) {
        int pieceSize = pieceLengthAt(pieceIndex);
        int blocks = (pieceSize + BLOCK_LENGTH - 1) / BLOCK_LENGTH;

        if (blockIndex < 0 || blockIndex >= blocks) {
            throw new IndexOutOfBoundsException(
                    "invalid block index: " + blockIndex);
        }

        return Math.min(
                BLOCK_LENGTH,
                pieceSize - blockIndex * BLOCK_LENGTH);
    }

    private void checkPieceIndex(int pieceIndex) {
        if (pieceIndex < 0 || pieceIndex >= numPieces) {
            throw new IndexOutOfBoundsException(
                    "invalid piece index: " + pieceIndex);
        }
    }
}
