package com.microservice.bittorentservice.business.handlers;
import java.nio.ByteBuffer;

public class CancelHandler {
    public byte[] buildCancel(int index, int begin, int length) {
        ByteBuffer buffer = ByteBuffer.allocate(12);
        buffer.putInt(index);
        buffer.putInt(begin);
        buffer.putInt(length);
        return buffer.array();
    }

    public RequestPayload parseCancel(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        return new RequestPayload(buffer.getInt(), buffer.getInt(), buffer.getInt());
    }
}