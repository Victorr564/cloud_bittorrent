package com.microservice.bittorentservice.business.peer;

import java.io.IOException;

public interface PeerObserver {
    void onPeerPieceChanged(PeerConnection peer) throws IOException;
    public void onPeerUnchoked(PeerConnection peer);
    public void onPeerChoked(PeerConnection peer);
}
