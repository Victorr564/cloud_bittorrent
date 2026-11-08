package com.microservice.bittorentservice.business.handlers;

import java.nio.ByteBuffer;

public class RequestHandler {
    public byte[] buildRequest(int index, int begin, int length) {
        ByteBuffer buffer = ByteBuffer.allocate(12);
        buffer.putInt(index);
        buffer.putInt(begin);
        buffer.putInt(length);
        return buffer.array();
    }

    public RequestPayload parseRequest(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        int index = buffer.getInt();
        int begin = buffer.getInt();
        int length = buffer.getInt();
        return new RequestPayload(index, begin, length);
    }

    public boolean handleRequest(RequestPayload requestPayload,boolean[] havePieces)
    {
            if((havePieces[requestPayload.index()]) && (requestPayload.begin()%16384==0) && (requestPayload.length()==16384))   //begin must be multiple of 16KB since a block is always 16KB, and length must always be 16384
            {
                return true;
            }
            else
            {
                return false; //message not compliant, drop it
            }
    }
}