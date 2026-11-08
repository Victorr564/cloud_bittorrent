package com.microservice.bittorentservice.business.handlers;

import java.nio.ByteBuffer;

public class PieceHandler {
    public byte[] buildPiece(int index, int begin, byte[] block) {
        ByteBuffer buffer = ByteBuffer.allocate(8 + block.length);
        buffer.putInt(index);
        buffer.putInt(begin);
        buffer.put(block);
        return buffer.array();
    }

    public PiecePayload parsePiece(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        int index = buffer.getInt();
        int begin = buffer.getInt();
        byte[] block = new byte[payload.length - 8];
        buffer.get(block);
        return new PiecePayload(index, begin, block);
    }
    public void handlePiece(PiecePayload piecePayload)
    {

    }
}
