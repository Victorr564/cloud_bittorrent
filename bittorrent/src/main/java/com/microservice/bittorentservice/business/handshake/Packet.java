package com.microservice.bittorentservice.business.handshake;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class Packet {
    public static ByteBuffer createPacket(byte[] infoHash, byte[] peerId)
    {
        ByteBuffer buffer=ByteBuffer.allocate(68);
        buffer.order(ByteOrder.BIG_ENDIAN);

        byte pstrlen = 19;
        byte[] pstr = "BitTorrent protocol".getBytes(StandardCharsets.US_ASCII);

        buffer.put(pstrlen);
        buffer.put(pstr);
        buffer.put(new byte[8]);
        buffer.put(infoHash,0,20);
        buffer.put(peerId,0,20);

        return buffer;
    }

}
