package com.microservice.bittorentservice.business.handlers;

public class BitfieldHandler {
    public byte[] buildBitfield(boolean[] havePieces)
    {
        int numPieces = havePieces.length;
        int numBytes = (numPieces + 7) / 8; // round up
        byte[] bitfield = new byte[numBytes];

        for (int i = 0; i < numPieces; i++) {
            if (havePieces[i]) {
                int byteIndex = i / 8;
                int bitOffset = 7 - (i % 8); // MSB first within each byte
                bitfield[byteIndex] |= (byte) (1 << bitOffset);
            }
        }
        return bitfield;
    }
    public boolean[] parseBitfield(byte[] bitfield, int numPieces)
    {

        boolean[] result = new boolean[numPieces];
        for (int i = 0; i < numPieces; i++) {
            int byteIndex = i / 8;
            int bitOffset = 7 - (i % 8);
            result[i] = ((bitfield[byteIndex] >> bitOffset) & 1) == 1;
        }
        return result;


    }
}
