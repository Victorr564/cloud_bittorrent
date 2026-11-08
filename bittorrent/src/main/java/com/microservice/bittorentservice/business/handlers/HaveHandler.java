package com.microservice.bittorentservice.business.handlers;

import java.nio.ByteBuffer;

public class HaveHandler {
    public byte[] buildHave(int pieceIndex) {
        return ByteBuffer.allocate(4).putInt(pieceIndex).array();
    }

    public int parseHave(byte[] payload) {
        return ByteBuffer.wrap(payload).getInt();
    }
}