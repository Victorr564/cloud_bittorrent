package com.microservice.bittorentservice.business.files;

import java.io.IOException;
import java.io.RandomAccessFile;

public class BlockReader implements IBlockReader {
    private final int pieceLength;
    private final FileStorage fileStorage;

    public BlockReader(int pieceLength, FileStorage fileStorage) {
        this.pieceLength = pieceLength;
        this.fileStorage = fileStorage;
    }

    @Override
    public byte[] readBlock(int index, int begin, int length) throws IOException {
        long globalOffset = (long) index * pieceLength + begin;

        // select() rejects negative or out-of-range requests before anything is allocated or opened
        // (a request from another peer is untrusted input)
        var segments = fileStorage.select(globalOffset, length);

        byte[] block = new byte[length];
        int destinationOffset = 0;

        for (FileSegment segment : segments) {
            // a new handle per call, so threads never share a file position
            try (RandomAccessFile raf = new RandomAccessFile(segment.path().toFile(), "r")) {
                raf.seek(segment.fileOffset());

                int segmentLength = Math.toIntExact(segment.length());
                raf.readFully(block, destinationOffset, segmentLength);

                destinationOffset += segmentLength;
            }
        }

        if (destinationOffset != length) {
            throw new IOException("Could not read the complete requested block");
        }

        return block;
    }
}