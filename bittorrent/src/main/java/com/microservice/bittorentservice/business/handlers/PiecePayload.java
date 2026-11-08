package com.microservice.bittorentservice.business.handlers;

public record PiecePayload(int index,int begin,byte[] block) {
}
