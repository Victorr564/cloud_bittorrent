package com.microservice.bittorentservice;

import com.microservice.bittorentservice.business.files.IBlockReader;
import com.microservice.bittorentservice.business.files.IBlockWriter;
import com.microservice.bittorentservice.business.handshake.Handshake;
import com.microservice.bittorentservice.business.handshake.HandshakeResult;
import com.microservice.bittorentservice.business.peer.*;
import com.microservice.bittorentservice.business.piece.*;
import com.microservice.bittorentservice.model.TorrentInfo;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;

public class TorrentController implements PieceObserver, PeerObserver {
    private final TorrentInfo info;
    private final PieceManager pieceManager;
    private final List<PeerConnection> peers = new CopyOnWriteArrayList<>();
    private final byte[] myPeerId;
    private final PieceTracker pieceTracker;
    private final IBlockReader blockReader;
    private final IBlockWriter blockWriter;
    private final Semaphore requestWindow;
    private static final int MAX_REQUESTS = 5;
    private static final int MAX_PEERS = 30;
    private final IPieceScheduler pieceScheduler;
    private final PeerChoker choker = new PeerChoker(peers);
    // peer -> pieces we requested from it that are not verified yet
    private final Map<PeerConnection, Set<Integer>> inFlight = new ConcurrentHashMap<>();

    // set by start(); peers that arrive later (from the tracker through Kafka) are connected through it
    private volatile Handshake handshake;
    // "host:port" of every address we already tried, so a repeated tracker message does nothing
    private final Set<String> attemptedPeers = ConcurrentHashMap.newKeySet();

    public TorrentController(TorrentInfo info, PieceManager pieceManager, IPieceScheduler pieceScheduler, byte[] peerId,
                             IBlockReader blockReader, IBlockWriter blockWriter) {
        this.info = info;
        this.pieceManager = pieceManager;
        this.myPeerId = peerId;
        this.pieceTracker = new PieceTracker(info);
        pieceManager.addObserver(this);
        this.blockReader = blockReader;
        this.blockWriter = blockWriter;
        requestWindow = new Semaphore(MAX_REQUESTS);
        this.pieceScheduler = pieceScheduler;
    }

    private void addPeer(PeerConnection peer) {
        peers.add(peer);
    }

    public void start(List<PeerAddress> peersToConnect) throws IOException {

        Handshake handshake = new Handshake(info.infoHash(), myPeerId);
        this.handshake = handshake;
        handshake.setOnConnected(handshakeResult -> {
            System.out.println("Peer connected!");
            try {
                PeerConnection pc = newPeer(handshakeResult);
                runPeer(pc); // blocks this virtual thread until the connection ends
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        handshake.start();
        System.out.println("Listening on port " + handshake.getPort());

        connectToPeers(peersToConnect);
        this.choker.start();
    }

    public void stop() {
        choker.stop();
    }

    /** Connects to every address we have not tried yet, each from its own virtual thread so a dead peer cannot block the others. */
    public void connectToPeers(List<PeerAddress> addresses) {
        Handshake h = this.handshake;
        if (h == null) {
            System.out.println("Client not started yet, ignoring " + addresses.size() + " peers");
            return;
        }
        for (PeerAddress addr : addresses) {
            if (peers.size() >= MAX_PEERS) return;
            if (!attemptedPeers.add(addr.host() + ":" + addr.port())) continue; // already tried this address
            Thread.ofVirtual().start(() -> connectOutbound(h, addr));
        }
    }

    private void connectOutbound(Handshake handshake, PeerAddress addr) {
        try {
            Optional<HandshakeResult> result = handshake.connectToPeer(addr.host(), addr.port(), null);
            if (result.isEmpty()) {
                System.out.println("Handshake failed with " + addr.host() + ":" + addr.port());
                return;
            }
            runPeer(newPeer(result.get())); // blocks this virtual thread for the life of the connection
        } catch (IOException e) {
            System.out.println("Could not connect to " + addr.host() + ":" + addr.port() + ": " + e.getMessage());
        }
    }

    private PeerConnection newPeer(HandshakeResult result) throws IOException {
        PeerConnection pc = new PeerConnection(info, blockReader, blockWriter, pieceManager, result, pieceTracker, new TransferRate(), new TransferRate());
        pc.addObserver(this);
        addPeer(pc);
        return pc;
    }

    // one virtual thread per connection: send our bitfield, read until the connection dies, then clean up
    private void runPeer(PeerConnection pc) throws IOException {
        try {
            pc.sendInitialBitfield();
            pc.receiveMessages();
        } catch (IOException | NoSuchAlgorithmException e) {
            System.out.println("Peer connection ended: " + e);
        } finally {
            removePeer(pc);
        }
    }

    private void removePeer(PeerConnection peer) throws IOException {
        peers.remove(peer);
        releaseInFlight(peer);
        peer.stop();
        requestNextPiece(); // whatever it was holding can go to someone else
    }

    // gives back every piece requested from this peer, and the permit each one held
    private void releaseInFlight(PeerConnection peer) {
        Set<Integer> pieces = inFlight.remove(peer);
        if (pieces == null) return;
        for (int piece : pieces) {
            pieceTracker.release(piece);
            requestWindow.release();
        }
    }

    // the piece is done (verified or failed), so it is no longer owned by any peer
    private void forgetInFlight(int pieceIndex) {
        for (Set<Integer> pieces : inFlight.values()) {
            pieces.remove(pieceIndex);
        }
    }

    @Override
    public void onPieceVerified(int index) {
        forgetInFlight(index);
        requestWindow.release();
        broadcastHave(index);
        refreshInterest();
        requestNextPiece();
    }

    @Override
    public void onPieceVerifiedFailed(int index) {
        forgetInFlight(index);
        pieceTracker.release(index); // otherwise it stays "in progress" and is never retried
        requestWindow.release();
        requestNextPiece();
    }

    public void requestNextPiece() {
        while (requestWindow.tryAcquire()) {
            if (!requestNext()) {
                requestWindow.release();
                break;
            }
        }
    }

    @Override
    public void onPeerPieceChanged(PeerConnection peerConnection) throws IOException {
        updateInterest(peerConnection);
        requestNextPiece();
    }

    @Override
    public void onPeerUnchoked(PeerConnection peer) {
        requestNextPiece();
    }

    @Override
    public void onPeerChoked(PeerConnection peer) {
        releaseInFlight(peer);
        requestNextPiece(); // re-request from peers that aren't choking us
    }

    private Optional<PieceTarget> schedulePieces() {
        List<PeerConnection> usable = peers.stream().filter(p -> !p.isPeerChoking()).toList();
        return pieceScheduler.schedulePieces(pieceManager.getHavePieces(), usable, pieceTracker);
    }

    private void updateInterest(PeerConnection peer) throws IOException {
        boolean[] have = pieceManager.getHavePieces();
        boolean[] theirs = peer.getPeerPieces();
        boolean wantsSomething = false;
        for (int i = 0; i < Math.min(have.length, theirs.length); i++) {
            if (theirs[i] && !have[i]) { wantsSomething = true; break; }
        }
        if (wantsSomething)
            peer.sendInterested();
        else
            peer.sendNotInterested();
    }

    // a verified piece can make us lose interest in peers whose only useful piece it was
    private void refreshInterest() {
        for (PeerConnection p : peers) {
            try {
                updateInterest(p);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean requestNext() {
        while (true) {
            Optional<PieceTarget> target = schedulePieces();
            if (target.isEmpty()) {
                System.out.println("Nothing left to request.");
                return false;
            }

            PieceTarget t = target.get();
            int pieceIndex = t.pieceIndex();

            if (!pieceTracker.tryClaim(pieceIndex)) {
                continue; // another thread took it; it is in progress now, so the scheduler skips it
            }
            inFlight.computeIfAbsent(t.peer(), p -> ConcurrentHashMap.newKeySet()).add(pieceIndex);

            try {
                for (int blockIndex = 0; blockIndex < info.blocksInPiece(pieceIndex); blockIndex++) {
                    t.peer().sendRequest(
                            pieceIndex,
                            blockIndex * TorrentInfo.BLOCK_LENGTH,
                            info.blockLengthAt(pieceIndex, blockIndex));
                }
            } catch (IOException e) {
                e.printStackTrace();
                pieceTracker.release(pieceIndex); // let it be rescheduled

                Set<Integer> pieces = inFlight.get(t.peer());
                if (pieces != null) pieces.remove(pieceIndex); // undo the record
                return false;
            }
            return true;
        }
    }

    private void broadcastHave(int pieceIndex) {
        for (PeerConnection pc : peers) {
            try {
                pc.sendHave(pieceIndex);
            } catch (IOException e) {
                e.printStackTrace(); // one dead peer shouldn't stop the rest
            }
        }
    }
}