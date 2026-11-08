package com.microservice.bittorentservice.business.handshake;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class Handshake {

    private byte[] infoHash;
    private byte[] peerId;
    private ServerSocket serverSocket=null;
    private final int port_range_start=6881;
    private final int port_range_end=6889;
    private static final int HANDSHAKE_LEN = 68;
    private static final int INFO_HASH_OFFSET = 28; // 1 (pstrlen) + 19 (pstr) + 8 (reserved)
    private static final int PEER_ID_OFFSET = 48;  // INFO_HASH_OFFSET + 20
    private int port=-1;
    private Consumer<HandshakeResult> onConnected;

    public Handshake(byte[] infoHash,byte[] peerId){
        this.infoHash= infoHash;
        this.peerId=peerId;
    }
    public int getPort()
    {
        return this.port;
    }
    public void start() throws IOException {
        for (int candidatePort = port_range_start; candidatePort <= port_range_end; candidatePort++) {
            try {
                serverSocket = new ServerSocket(candidatePort);
                this.port = candidatePort;
                break;
            } catch (BindException e) {
                System.out.println("Port already in use, try next port");
            }
        }
        if (serverSocket == null) {
            throw new IOException("Unable to bind to any port in range " + port_range_start + "-" + port_range_end);
        }
        Thread.ofVirtual().start(this::startListening);
    }
    // Listen for other peers
    private void startListening() {

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {

            while (!serverSocket.isClosed()) {
                try {
                    Socket socket = serverSocket.accept();
                    executor.submit(() -> handlePeer(socket));
                } catch (IOException e) {
                    if (serverSocket.isClosed()) {
                         break;
                    }
                    e.printStackTrace();
                }
            }
        }
    }

    public void setOnConnected(Consumer<HandshakeResult> onConnected) {
        this.onConnected = onConnected;
    }

    // Handle an incoming connection
    private Optional<HandshakeResult> handlePeer(Socket socket) {

        try {
            socket.setSoTimeout(5000);
            var in = socket.getInputStream();
            byte[] handshake = new byte[HANDSHAKE_LEN];
            int read = in.readNBytes(handshake, 0, HANDSHAKE_LEN);
            if (read < HANDSHAKE_LEN) {
                System.out.println("Disconnected mid-handshake");
                socket.close();
                return Optional.empty();
            }

            byte[] receivedInfoHash = Arrays.copyOfRange(handshake, INFO_HASH_OFFSET, INFO_HASH_OFFSET + 20);
            byte[] receivedPeerId   = Arrays.copyOfRange(handshake, PEER_ID_OFFSET, PEER_ID_OFFSET + 20);

            if (!Arrays.equals(receivedInfoHash, infoHash)) {
                System.out.println("Info hash mismatch, closing connection.");
                socket.close();
                return Optional.empty();
            }

            var out = socket.getOutputStream();
            out.write(Packet.createPacket(infoHash, peerId).array());
            out.flush();

            if (onConnected != null) {
                onConnected.accept(new HandshakeResult(socket, receivedPeerId));
            }
            return Optional.of(new HandshakeResult(socket, receivedPeerId));
        } catch (IOException e) {
            e.printStackTrace();
            try { socket.close(); } catch (IOException ignored) {}
            return Optional.empty();
        }
    }

    public Optional<HandshakeResult> connectToPeer(String host, int port, byte[] expectedPeerId) throws IOException {
        Socket socket = new Socket();

        try {
            socket.connect(new java.net.InetSocketAddress(host, port), 5000);
            socket.setSoTimeout(5000);
            var out = socket.getOutputStream();
            out.write(Packet.createPacket(infoHash, peerId).array());
            out.flush();

            byte[] response = new byte[HANDSHAKE_LEN];
            int read = socket.getInputStream().readNBytes(response, 0, HANDSHAKE_LEN);

            if (read < HANDSHAKE_LEN) {
                System.out.println("Peer closed connection without full handshake.");
                socket.close();
                return Optional.empty();
            }

            byte[] responseInfoHash = Arrays.copyOfRange(response, INFO_HASH_OFFSET, INFO_HASH_OFFSET + 20);
            byte[] responsePeerId   = Arrays.copyOfRange(response, PEER_ID_OFFSET, PEER_ID_OFFSET + 20);

            boolean infoHashOk = Arrays.equals(responseInfoHash, infoHash);
            boolean peerIdOk   = expectedPeerId == null || Arrays.equals(responsePeerId, expectedPeerId);

            if (!infoHashOk || !peerIdOk) {
                System.out.println("Handshake mismatch (info hash or peer id), severing connection.");
                socket.close();
                return Optional.empty();
            }

            System.out.println("Received handshake, " + read + " bytes");
            return Optional.of(new HandshakeResult(socket, responsePeerId));
        } catch (IOException e) {
            socket.close();
            throw e;
        }
    }

    public void stop() throws IOException {
        serverSocket.close();
    }
}