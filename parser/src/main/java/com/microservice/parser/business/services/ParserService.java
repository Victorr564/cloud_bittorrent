package com.microservice.parser.business.services;

import com.dampcake.bencode.Bencode;
import com.dampcake.bencode.Type;
import com.microservice.parser.business.interfaces.ParserInterface;
import com.microservice.parser.business.models.TorrentData;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;

@Service
public class ParserService implements ParserInterface {

    @Override
    public TorrentData parseTorrent(byte[] file) {
        try {
            Bencode rawBencode = new Bencode(true);

            Map<String, Object> dict = rawBencode.decode(file, Type.DICTIONARY);
            Map<String, Object> info = (Map<String, Object>) dict.get("info");

            byte[] infoBencoded = rawBencode.encode(info);
            MessageDigest digest = MessageDigest.getInstance("SHA-1");
            byte[] infoHash = digest.digest(infoBencoded);

            String announce = toUtf8String(dict.get("announce"));
            String name = toUtf8String(info.get("name"));
            byte[] pieces = toByteArray(info.get("pieces"));

            System.out.println("announce = " + announce);
            System.out.println("name = " + name);
            System.out.println("piece length = " + info.get("piece length"));

            Long length = (Long) info.get("length"); // null for multi-file torrents
            Long pieceLength = (Long) info.get("piece length");

            List<Map<String, Object>> files = (List<Map<String, Object>>) info.get("files"); // null for single-file torrents

            return new TorrentData(announce, infoHash, length, name, pieceLength, pieces, files);
        }
        catch(NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


        return null;
    }

    private static String toUtf8String(Object raw) {
        if (raw == null) return null;
        if (raw instanceof ByteBuffer) {
            return StandardCharsets.UTF_8.decode(((ByteBuffer) raw).duplicate()).toString();
        }
        if (raw instanceof byte[]) {
            return new String((byte[]) raw, StandardCharsets.UTF_8);
        }
        return raw.toString();
    }
    private static byte[] toByteArray(Object raw) {
        if (raw == null) return null;
        if (raw instanceof ByteBuffer) {
            ByteBuffer buf = ((ByteBuffer) raw).duplicate();
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            return bytes;
        }
        if (raw instanceof byte[]) {
            return (byte[]) raw;
        }
        throw new IllegalStateException("Unexpected type for binary field: " + raw.getClass());
    }
}