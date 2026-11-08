package com.microservice.bittorentservice.business.piece;

public interface PieceObserver {
    void onPieceVerified(int index);
    void onPieceVerifiedFailed(int index);
}
