package com.microservice.tracker.services;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.dampcake.bencode.Bencode;
import com.dampcake.bencode.Type;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microservice.tracker.interfaces.TrackerInterface;
import com.microservice.tracker.models.TorrentData;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public class TrackerService implements TrackerInterface {
    private static Map<String,Integer> peersMap=new HashMap<String,Integer>();
    private static final byte[] peerId = generatePeerId();
    @Override
    public void announce(TorrentData torrentData) throws IOException, InterruptedException {
        String url = torrentData.announceURL()
                + "?info_hash=" + urlEncodeBytes(torrentData.infoHash())
                + "&peer_id=" + urlEncodeBytes(peerId)
                + "&port=6881"
                + "&uploaded=0"
                + "&downloaded=0"
                + "&left=" + torrentData.length()
                + "&compact=1"
                + "&event=started";

        System.out.println("announce url = " + url);

        // HTTP/1.1 only: on plain http the default client also sends HTTP/2 upgrade headers,
        // and simple trackers sometimes answer those with a broken response
        HttpClient httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        HttpRequest httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(15))
                .header("User-Agent", "MicroTracker/0.1")
                .GET()
                .build();

        HttpResponse<byte[]> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofByteArray());
        System.out.println("status = " + response.statusCode());
        if (response.statusCode() != 200) {
            System.out.println("tracker did not answer with 200, giving up");
            return;
        }

        Bencode bencode = new Bencode(true);
        Map<String, Object> data = bencode.decode(response.body(), Type.DICTIONARY);

        // a tracker that rejects the announce answers with a "failure reason" instead of peers
        Object failure = data.get("failure reason");
        if (failure instanceof ByteBuffer reason) {
            System.out.println("tracker refused: " + StandardCharsets.UTF_8.decode(reason.duplicate()));
            return;
        }

        System.out.println("complete = " + data.get("complete"));
        System.out.println("incomplete = " + data.get("incomplete"));
        System.out.println("interval = " + data.get("interval"));

        // with compact=1 the peers are one byte string, 6 bytes per peer
        if (!(data.get("peers") instanceof ByteBuffer peersBuf)) {
            System.out.println("tracker returned no compact peer list: " + data.get("peers"));
            return;
        }
        byte[] peers = new byte[peersBuf.remaining()];
        peersBuf.get(peers);
        decodePeers(peers);
        System.out.println(peersMap);
    }
    @Override
    public void forwardPeers()
    {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        ObjectMapper objectMapper = new ObjectMapper();



        Producer<String, String> producer = null;
        try {
            // Create the producer
            producer = new KafkaProducer<>(props);

            String json = objectMapper.writeValueAsString(peersMap);
            // Create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("peers.updated", "key", json);

            // Send the record with a callback to handle exceptions
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        System.err.println("Error sending record: " + exception.getMessage());
                        exception.printStackTrace();
                    } else {
                        System.out.println("Record sent successfully to topic " + metadata.topic() +
                                " partition " + metadata.partition() + " at offset " + metadata.offset());
                    }
                }
            });

        } catch (Exception e) {
            System.err.println("Error creating or sending producer: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Close the producer
            if (producer != null) {
                try {
                    producer.close();
                } catch (Exception e) {
                    System.err.println("Error closing producer: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }


    }
    private static byte[] generatePeerId() {
        byte[] id = new byte[20];
        byte[] prefix = "-MT0001-".getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(prefix, 0, id, 0, prefix.length);
        byte[] rest = new byte[20 - prefix.length];
        new SecureRandom().nextBytes(rest);
        System.arraycopy(rest, 0, id, prefix.length, rest.length);
        return id;
    }
    private static void decodePeers(byte[] peers)
    {
        for (int i = 0; i + 6 <= peers.length; i += 6) {
            String ip = (peers[i] & 0xFF) + "." + (peers[i + 1] & 0xFF) + "."
                    + (peers[i + 2] & 0xFF) + "." + (peers[i + 3] & 0xFF);
            int port = ((peers[i + 4] & 0xFF) << 8) | (peers[i + 5] & 0xFF);
            peersMap.put(ip,port);
        }
    }
    private static String urlEncodeBytes(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            int v = b & 0xFF;
            // unreserved chars per RFC 3986 (and BitTorrent convention)
            if ((v >= 'A' && v <= 'Z') || (v >= 'a' && v <= 'z') ||
                    (v >= '0' && v <= '9') || v == '-' || v == '_' || v == '.' || v == '~') {
                sb.append((char) v);
            } else {
                sb.append('%').append(String.format("%02X", v));
            }
        }
        return sb.toString();
    }
}