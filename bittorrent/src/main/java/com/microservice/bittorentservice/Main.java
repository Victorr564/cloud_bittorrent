package com.microservice.bittorentservice;

import java.nio.file.Path;

/**
 * Manual test harness. Kafka must be running with a TorrentData message on torrent.parsed.
 * Peers come from two places: host:port arguments, and the tracker service through peers.updated.
 *
 * The first argument is the DOWNLOAD DIRECTORY. The torrent's files live directly in it
 * (a single-file torrent is one file named after the torrent), so a seeder's directory must already contain them.
 *
 * Seeder (a directory with the complete files):
 *     java ... Main /path/to/seed-dir
 *
 * Leecher (an empty or partial directory), optional extra peers on the command line:
 *     java ... Main /path/to/download-dir [127.0.0.1:PORT1 127.0.0.1:PORT2 ...]
 */
public class Main {

    public static void main(String[] args) throws Exception {
        String dir = args.length > 0 ? args[0] : System.getenv().getOrDefault("DOWNLOAD_DIR", "/download");

        Client client = new Client(Path.of(dir));

        for (int i = 1; i < args.length; i++) {
            String[] hp = args[i].split(":");
            client.addPeer(hp[0], Integer.parseInt(hp[1]));
        }

        Runtime.getRuntime().addShutdownHook(new Thread(client::stop));

        client.start(); // returns immediately, everything runs on virtual threads
        client.await(); // never returns on its own, so stopping happens in the shutdown hook
    }
}