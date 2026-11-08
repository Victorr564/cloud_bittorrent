package com.microservice.bittorentservice.business.messages;

import com.microservice.bittorentservice.business.handlers.BitfieldHandler;
import com.microservice.bittorentservice.business.handlers.CancelHandler;
import com.microservice.bittorentservice.business.handlers.HaveHandler;
import com.microservice.bittorentservice.business.handlers.PieceHandler;
import com.microservice.bittorentservice.business.handlers.RequestHandler;

public class MessageBuilder {
    private static final IMessageFramer messageFramer = new MessageFramer();
    private static final HaveHandler haveHandler = new HaveHandler();
    private static final BitfieldHandler bitfieldHandler = new BitfieldHandler();
    private static final RequestHandler requestHandler = new RequestHandler();
    private static final CancelHandler cancelHandler = new CancelHandler();
    private static final PieceHandler pieceHandler = new PieceHandler();

    public static byte[] ofChoke() {
        return messageFramer.frameMessage(MessageTypes.CHOKE, new byte[0]);
    }

    public static byte[] ofUnchoke() {
        return messageFramer.frameMessage(MessageTypes.UNCHOKE, new byte[0]);
    }

    public static byte[] ofInterested() {
        return messageFramer.frameMessage(MessageTypes.INTERESTED, new byte[0]);
    }

    public static byte[] ofNotInterested() {
        return messageFramer.frameMessage(MessageTypes.NOT_INTERESTED, new byte[0]);
    }

    public static byte[] ofHave(int pieceIndex) {
        byte[] payload = haveHandler.buildHave(pieceIndex);
        return messageFramer.frameMessage(MessageTypes.HAVE, payload);
    }

    public static byte[] ofBitfield(boolean[] havePieces) {
        byte[] payload = bitfieldHandler.buildBitfield(havePieces);
        return messageFramer.frameMessage(MessageTypes.BITFIELD, payload);
    }

    public static byte[] ofRequest(int index, int begin, int length) {
        byte[] payload = requestHandler.buildRequest(index, begin, length);
        return messageFramer.frameMessage(MessageTypes.REQUEST, payload);
    }

    public static byte[] ofPiece(int index, int begin, byte[] block) {
        byte[] payload = pieceHandler.buildPiece(index, begin, block);
        return messageFramer.frameMessage(MessageTypes.PIECE, payload);
    }

    public static byte[] ofCancel(int index, int begin, int length) {
        byte[] payload = cancelHandler.buildCancel(index, begin, length);
        return messageFramer.frameMessage(MessageTypes.CANCEL, payload);
    }
}