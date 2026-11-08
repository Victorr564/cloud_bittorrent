package com.microservice.bittorentservice;

import com.microservice.bittorentservice.business.files.BlockReader;
import com.microservice.bittorentservice.business.files.BlockWriter;
import com.microservice.bittorentservice.business.files.IBlockReader;
import com.microservice.bittorentservice.business.files.IBlockWriter;
import com.microservice.bittorentservice.business.files.FileStorage;
import com.microservice.bittorentservice.business.peer.PeerAddress;
import com.microservice.bittorentservice.business.piece.PieceManager;
import com.microservice.bittorentservice.business.piece.RarestFirstPieceScheduler;
import com.microservice.bittorentservice.kafka.config.KafkaConsumerComponent;
import com.microservice.bittorentservice.kafka.controllers.KafkaController;
import com.microservice.bittorentservice.model.TorrentData;
import com.microservice.bittorentservice.model.TorrentInfo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The public face of the library. It wires everything together so a caller needs four lines:
 *
 *     Client client = new Client(Path.of("downloads"));
 *     client.addPeer("127.0.0.1", 6881);   // optional, more peers can arrive any time
 *     client.start();                      // torrent + tracker peers come from Kafka
 *     client.await();                      // block until stop()
 *
 * Without Kafka, hand it the torrent yourself:  client.start(torrentData);
 *
 * One Client downloads one torrent (the first one it receives).
 * The torrent's files live directly in downloadDirectory.
 */
public class Client {
    private final Path downloadDirectory;
    private final byte[] peerId = generatePeerId();

    // completed once the torrent is wired and listening; peers added before that wait on it
    private final CompletableFuture<TorrentController> ready = new CompletableFuture<>();
    private final AtomicBoolean started = new AtomicBoolean();
    private final CountDownLatch stopped = new CountDownLatch(1);

    private volatile PieceManager pieceManager;
    private volatile KafkaConsumerComponent kafka;

    public Client(Path downloadDirectory) {
        this.downloadDirectory = downloadDirectory;
    }

    /** Connect to this peer. Safe to call before start(): it is connected as soon as the torrent is running. */
    public void addPeer(String host, int port) {
        addPeers(List.of(new PeerAddress(host, port)));
    }

    public void addPeers(List<PeerAddress> peers) {
        ready.thenAccept(controller -> controller.connectToPeers(peers))
                .exceptionally(e -> { e.printStackTrace(); return null; });
    }

    /** Non-blocking. Listens on Kafka: the first torrent on torrent.parsed starts the download, peers.updated feeds peers. */
    public void start() {
        KafkaConsumerComponent component = KafkaConsumerComponent.createService();
        this.kafka = component;
        KafkaController kafkaController = new KafkaController(component);

        System.out.println("Waiting for torrent on torrent.parsed ...");
        Thread.ofVirtual().start(() -> kafkaController.listen(
                (torrentId, data) -> {
                    try {
                        start(data);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                },
                this::addPeers));
    }

    /** Starts the download of this torrent. Only the first call does anything. */
    public void start(TorrentData data) throws IOException {
        if (!started.compareAndSet(false, true)) {
            System.out.println("Already downloading a torrent, ignoring another one");
            return;
        }
        try {
            TorrentInfo info = TorrentInfo.from(data);

            // validates every path and computes every offset, without touching the disk yet
            FileStorage fileStorage = new FileStorage(filesOf(data, info), downloadDirectory);

            // the metadata has to agree with itself before anything is created
            long pieceLength = info.pieceLength();
            long expectedPieces = (fileStorage.totalLength() + pieceLength - 1) / pieceLength;
            if (expectedPieces != info.numPieces()) {
                throw new IllegalStateException("Torrent says " + info.numPieces() + " pieces, but its files add up to "
                        + fileStorage.totalLength() + " bytes (" + expectedPieces + " pieces)");
            }

            fileStorage.createFiles(); // folders and files at full size, so every read and write has a target

            IBlockReader blockReader = new BlockReader(info.pieceLength(), fileStorage);
            IBlockWriter blockWriter = new BlockWriter(info.pieceLength(), fileStorage);
            PieceManager pieceManager = new PieceManager(info, blockReader); // hashes what is already on disk
            this.pieceManager = pieceManager;

            TorrentController controller = new TorrentController(
                    info, pieceManager, new RarestFirstPieceScheduler(), peerId, blockReader, blockWriter);
            controller.start(List.of()); // listens for inbound peers

            // only now: connectToPeers needs the handshake that start() creates.
            // Peers added earlier are connected right here.
            ready.complete(controller);
        } catch (IOException e) {
            ready.completeExceptionally(e);
            throw e;
        } catch (Exception e) {
            ready.completeExceptionally(e);
            throw new IllegalStateException("Could not start the torrent", e);
        }
    }

    /** 0.0 to 1.0, the part of the torrent that is verified on disk. */
    public double progress() {
        PieceManager pm = pieceManager;
        if (pm == null) return 0;
        boolean[] have = pm.getHavePieces();
        if (have.length == 0) return 0;
        int done = 0;
        for (boolean b : have) if (b) done++;
        return (double) done / have.length;
    }

    public boolean isComplete() {
        return progress() >= 1.0;
    }

    /** Blocks until stop() is called. */
    public void await() throws InterruptedException {
        stopped.await();
    }

    public void stop() {
        ready.thenAccept(TorrentController::stop);
        stopped.countDown();
    }

    // a single-file torrent has no files list: one entry, named after the torrent (the name is only a suggestion)
    private static List<Map<String, Object>> filesOf(TorrentData data, TorrentInfo info) {
        List<Map<String, Object>> files = info.files();
        if (files != null && !files.isEmpty()) return files;

        Long length = data.length();
        if (length == null) {
            throw new IllegalStateException("Torrent has neither a files list nor a length");
        }
        String name = (data.name() == null || data.name().isBlank())
                ? HexFormat.of().formatHex(data.infoHash())
                : data.name();
        return List.of(Map.of("path", List.of(name), "length", length));
    }

    // 20 bytes: client prefix + random bytes (must be unique per process)
    private static byte[] generatePeerId() {
        byte[] id = new byte[20];
        byte[] prefix = "-JT0001-".getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(prefix, 0, id, 0, prefix.length);
        byte[] rest = new byte[20 - prefix.length];
        new SecureRandom().nextBytes(rest);
        System.arraycopy(rest, 0, id, prefix.length, rest.length);
        return id;
    }
}