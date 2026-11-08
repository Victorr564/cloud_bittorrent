package com.microservice.bittorentservice.business.peer;

public class TransferRate {
    private long totalBytes = 0;
    private long windowStartBytes = 0;
    private long windowStartNanos = System.nanoTime();   // window starts at construction
    private double rate = 0.0;

    public synchronized void update(long byteCount) {
        totalBytes += byteCount;
    }

    /** Bytes/sec since the previous call, then starts a new window. */
    public synchronized double computeRate() {
        long now = System.nanoTime();
        double elapsed = (now - windowStartNanos) / 1_000_000_000.0;
        if (elapsed <= 0) return rate;

        rate = (totalBytes - windowStartBytes) / elapsed;
        windowStartBytes = totalBytes;
        windowStartNanos = now;
        return rate;
    }

    public synchronized void reset() {
        totalBytes = 0;
        windowStartBytes = 0;
        windowStartNanos = System.nanoTime();
        rate = 0.0;
    }
}
