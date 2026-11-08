package com.microservice.bittorentservice.business.files;

import java.io.IOException;
import java.io.RandomAccessFile;

public class BlockWriter implements IBlockWriter {
    private final int pieceLength;
    private final FileStorage fileStorage;

    public BlockWriter(int pieceLength, FileStorage fileStorage) {
        this.pieceLength = pieceLength;
        this.fileStorage = fileStorage;
    }

    @Override
    public void writeBlock(int index, int begin, byte[] block) throws IOException {
        long globalOffset = (long) index * pieceLength + begin;

        // select() rejects out-of-range writes before anything is touched, so a block is never half-written
        // because it runs past the end of the torrent
        var segments = fileStorage.select(globalOffset, block.length);

        int sourceOffset = 0;
        for (FileSegment segment : segments) {
            // a new handle per call: two peer threads writing different blocks of the same file never share a position
            try (RandomAccessFile raf = new RandomAccessFile(segment.path().toFile(), "rw")) {
                raf.seek(segment.fileOffset());

                int segmentLength = Math.toIntExact(segment.length());
                raf.write(block, sourceOffset, segmentLength);

                sourceOffset += segmentLength;
            }
        }
    }
}