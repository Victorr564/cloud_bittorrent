/**
 class for sending/receveing messages between two peers ( a tcp connection)
 one peer in the swarm = one object of this class created
 also contains peerPieces, which represents the pieces that the peer which you conneced to has
 */

package com.microservice.bittorentservice.business.peer;

import com.microservice.bittorentservice.business.handshake.HandshakeResult;
import com.microservice.bittorentservice.business.files.IBlockReader;
import com.microservice.bittorentservice.business.files.IBlockWriter;
import com.microservice.bittorentservice.business.messages.MessageBuilder;
import com.microservice.bittorentservice.business.handlers.*;
import com.microservice.bittorentservice.model.TorrentInfo;
import com.microservice.bittorentservice.business.piece.PieceManager;
import com.microservice.bittorentservice.business.piece.PieceTracker;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class PeerConnection {
    private final RequestHandler requestHandler = new RequestHandler();
    private final CancelHandler cancelHandler = new CancelHandler();
    private final BitfieldHandler bitfieldHandler = new BitfieldHandler();
    private final PieceHandler pieceHandler = new PieceHandler();
    private final HaveHandler haveHandler = new HaveHandler();
    private final PieceManager pieceManager;
    private final TransferRate downloadRate;
    private final TransferRate uploadRate;
    private final PieceTracker pieceTracker;

    private final Socket socket;
    private final OutputStream out;
    private final DataInputStream dis;

    private volatile boolean[] peerPieces; //the peer which we connect to s pieces
    private final TorrentInfo info;
    private final IBlockReader blockReader;
    private final IBlockWriter blockWriter;
    private final List<PeerObserver> observers = new CopyOnWriteArrayList<>();

    // OUR state toward the peer (connections start choked and not interested)
    private volatile boolean chokeState = true;                          // we are choking the peer
    private final AtomicBoolean interestState = new AtomicBoolean(false); // we are interested in the peer

    // the PEER's state toward us
    private volatile boolean peerChokeState = true;      // the peer is choking us
    private volatile boolean peerInterestState = false;  // the peer is interested in us

    public PeerConnection(TorrentInfo info,IBlockReader blockReader,IBlockWriter blockWriter,PieceManager pieceManager, HandshakeResult handshakeResult,
                          PieceTracker pieceTracker,TransferRate downloadRate, TransferRate uploadRate) throws IOException {

        this.info = info;
        this.pieceManager=pieceManager;
        this.blockReader=blockReader;
        this.blockWriter=blockWriter;
        this.pieceTracker = pieceTracker;
        this.socket = handshakeResult.socket();
        this.out = socket.getOutputStream();
        this.dis = new DataInputStream(socket.getInputStream());
        this.peerPieces = new boolean[info.numPieces()];
        this.downloadRate=downloadRate;
        this.uploadRate=uploadRate;
    }
    public void addObserver(PeerObserver o) { observers.add(o); }
    private void notifyPeerPieceChange() throws IOException {
        for (PeerObserver o : observers) o.onPeerPieceChanged(this);
    }
    private void notifyPeerChoked() {
        for (PeerObserver o : observers) o.onPeerChoked(this);
    }
    private void notifyPeerUnchoked() {
        for (PeerObserver o : observers) o.onPeerUnchoked(this);
    }
    public void sendInitialBitfield() throws IOException {
        out.write(MessageBuilder.ofBitfield(pieceManager.getHavePieces()));
        out.flush();
    }

    public void sendRequest(int index, int begin, int length) throws IOException {
        out.write(MessageBuilder.ofRequest(index, begin, length));
        out.flush();
    }
    public void sendInterested() throws IOException {
        if (!interestState.compareAndSet(false, true)) return;   // only send on a change
        out.write(MessageBuilder.ofInterested());
        out.flush();
    }

    public void sendNotInterested() throws IOException {
        if (!interestState.compareAndSet(true, false)) return;
        out.write(MessageBuilder.ofNotInterested());
        out.flush();
    }
    public void sendChoke() throws IOException
    {
        chokeState = true;
        out.write(MessageBuilder.ofChoke());
        out.flush();
    }
    public void sendUnchoke() throws IOException
    {
        chokeState = false;   // flip first, so a request arriving right after the unchoke is served
        out.write(MessageBuilder.ofUnchoke());
        out.flush();
    }
    public void sendHave(int index) throws IOException
    {
        out.write(MessageBuilder.ofHave(index));
        out.flush();
    }

    // our state toward the peer
    public boolean isChoking()
    {
        return this.chokeState;
    }
    public boolean isInterested()
    {
        return this.interestState.get();
    }

    // the peer's state toward us
    public boolean isPeerChoking()
    {
        return this.peerChokeState;
    }
    public boolean isPeerInterested()
    {
        return this.peerInterestState;
    }

    public void resetRates()
    {
        this.downloadRate.reset();
        this.uploadRate.reset();
    }
    public void receiveMessages() throws IOException, NoSuchAlgorithmException {
        while (!socket.isClosed()) {
            System.out.println("Loop iteration starting, socket closed=" + socket.isClosed());
            int length = dis.readInt();
            if (length == 0) {
                // keepalive message
                continue;
            }

            byte id = dis.readByte();
            System.out.println("--> HEADER READ: length=" + length + ", id=" + id);

            RequestPayload requestPayload;
            PiecePayload piecePayload;
            switch (id) {
                case 0: //choke message, the peer is now choking us
                    this.peerChokeState = true;
                    this.notifyPeerChoked();
                    break;
                case 1: //unchoke message, the peer is now serving us
                    this.peerChokeState = false;
                    this.notifyPeerUnchoked();
                    break;
                case 2: //interested message, the peer wants our pieces
                    this.peerInterestState = true;
                    break;
                case 3: //not interested message
                    this.peerInterestState = false;
                    break;
                case 4: // have message, broadcasted, send to all relevant peers except yourself
                    int pieceIndex= haveHandler.parseHave(dis.readNBytes(4));
                    peerPieces[pieceIndex]=true;
                    this.notifyPeerPieceChange();
                    break;
                case 5: //bitfield message
                    byte[] bitfieldPayload = dis.readNBytes(length - 1);
                    peerPieces = bitfieldHandler.parseBitfield(bitfieldPayload, info.numPieces());
                    this.notifyPeerPieceChange();
                    break;

                case 6: // request message
                    requestPayload = requestHandler.parseRequest(dis.readNBytes(12));
                    if(chokeState)   // we are choking this peer, ignore its request
                        break;
                    System.out.println("Received REQUEST: index=" + requestPayload.index() + " begin=" + requestPayload.begin() + " length=" + requestPayload.length());
                    if (requestHandler.handleRequest(requestPayload, pieceManager.getHavePieces())) {
                        byte[] blockBytes = blockReader.readBlock(
                                requestPayload.index(), requestPayload.begin(), requestPayload.length());
                        out.write(MessageBuilder.ofPiece(requestPayload.index(), requestPayload.begin(), blockBytes));
                        out.flush();
                        uploadRate.update(blockBytes.length);
                        System.out.println("Sent PIECE: index=" + requestPayload.index() + " begin=" + requestPayload.begin() + " block length=" + blockBytes.length);
                    }
                    else {
                        System.out.println("REQUEST rejected by handleRequest validation");
                    }
                    break;
                case 7: // piece message
                    piecePayload = pieceHandler.parsePiece(dis.readNBytes(length - 1));
                    this.downloadRate.update(piecePayload.block().length);
                    blockWriter.writeBlock(piecePayload.index(), piecePayload.begin(), piecePayload.block());

                    int pIndex = piecePayload.index();
                    int blockIndex = piecePayload.begin() / TorrentInfo.BLOCK_LENGTH;

                    if (pieceTracker.registerBlock(pIndex, blockIndex)) {
                        pieceManager.onPiece(piecePayload.index());
                    }
                    break;
                case 8: // cancel message
                    requestPayload = cancelHandler.parseCancel(dis.readNBytes(12)); // endgame mode, later
                    break;
            }
        }
    }
    public TransferRate getDownloadRate()
    {
        return this.downloadRate;
    }
    public TransferRate getUploadRate()
    {
        return this.uploadRate;
    }
    public boolean[] getPeerPieces() {
        return peerPieces;
    }

    public boolean isSeeding() {
        for (boolean has : pieceManager.getHavePieces()) {
            if (!has) return false;
        }
        return true;
    }
    public void stop() throws IOException {
        socket.close();
    }
}