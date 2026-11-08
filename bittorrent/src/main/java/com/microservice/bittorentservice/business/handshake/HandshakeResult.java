package com.microservice.bittorentservice.business.handshake;

import java.net.Socket;

public record HandshakeResult(Socket socket,byte[] peerId) {
}
