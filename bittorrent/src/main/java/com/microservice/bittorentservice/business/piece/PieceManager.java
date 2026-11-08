package com.microservice.bittorentservice.business.piece;

import com.microservice.bittorentservice.Sha1Hasher;
import com.microservice.bittorentservice.business.files.IBlockReader;
import com.microservice.bittorentservice.model.TorrentInfo;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class PieceManager {
    private final TorrentInfo info;
    private final boolean[] havePieces;
    private final Object lock = new Object(); // guards havePieces after construction
    private final List<PieceObserver> observers = new CopyOnWriteArrayList<>();
    private final PieceVerifier pieceVerifier;
    private final IBlockReader blockReader;

    public PieceManager(TorrentInfo info, IBlockReader blockReader) {
        this.info = info;
        this.havePieces = new boolean[info.numPieces()];
        this.pieceVerifier = new PieceVerifier(info);
        this.blockReader = blockReader;
        constructPieces(); // last, so everything it needs is initialized
    }

    public void addObserver(PieceObserver pieceObserver) {
        observers.add(pieceObserver);
    }

    /**
     * Hashes what is already on disk, piece by piece, and marks the pieces that match.
     * Every piece is read through the block reader, so it works for any number of files, and a piece that
     * starts in one file and ends in the next is just another read. A piece that cannot be read counts as missing.
     */
    private void constructPieces() {
        byte[][] hashes = info.pieceHashes(); // fetched once, not once per piece
        int have = 0;
        int unreadable = 0;
        try {
            for (int i = 0; i < info.numPieces(); i++) {
                try {
                    byte[] piece = blockReader.readBlock(i, 0, info.pieceLengthAt(i));
                    if (Arrays.equals(Sha1Hasher.sha1(piece), hashes[i])) {
                        havePieces[i] = true; // constructor only, nobody else can see this object yet
                        have++;
                    }
                } catch (IOException e) {
                    unreadable++;
                }
            }
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Have " + have + " of " + info.numPieces() + " pieces on disk"
                + (unreadable > 0 ? " (" + unreadable + " could not be read)" : ""));
    }

    public void onPiece(int pieceIndex) throws NoSuchAlgorithmException, IOException { // called when every block of the piece is in
        byte[] piece = blockReader.readBlock(pieceIndex, 0, info.pieceLengthAt(pieceIndex));
        if (pieceVerifier.verifyPiece(piece, pieceIndex)) {
            synchronized (lock) {
                havePieces[pieceIndex] = true;
            }
            for (PieceObserver observer : observers) {
                observer.onPieceVerified(pieceIndex);
            }
        } else {
            for (PieceObserver observer : observers) {
                observer.onPieceVerifiedFailed(pieceIndex);
            }
        }
    }

    /** A snapshot: safe to loop over while other threads verify pieces. */
    public boolean[] getHavePieces() {
        synchronized (lock) {
            return havePieces.clone();
        }
    }

}