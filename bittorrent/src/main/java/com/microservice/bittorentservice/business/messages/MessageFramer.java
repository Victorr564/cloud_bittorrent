package com.microservice.bittorentservice.business.messages;

import java.nio.ByteBuffer;

public class MessageFramer implements IMessageFramer{
    @Override
    public byte[] frameMessage(MessageTypes messageType, byte[] payload) {
        int length = 1 + payload.length;
        ByteBuffer buffer = ByteBuffer.allocate(4 + length);
        buffer.putInt(length);
        buffer.put((byte) messageType.ordinal());
        buffer.put(payload);
        return buffer.array();
    }
}
