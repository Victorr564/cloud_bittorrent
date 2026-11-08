package com.microservice.bittorentservice.business.messages;

public interface IMessageFramer {
    byte[] frameMessage(MessageTypes messageType, byte[] payload);
}
