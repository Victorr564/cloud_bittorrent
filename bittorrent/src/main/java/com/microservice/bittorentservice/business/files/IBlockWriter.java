package com.microservice.bittorentservice.business.files;

import java.io.IOException;

public interface IBlockWriter {
    void writeBlock(int index, int begin, byte[] blockBytes) throws IOException;
}
