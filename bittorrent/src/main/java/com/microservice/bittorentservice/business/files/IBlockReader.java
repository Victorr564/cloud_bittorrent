package com.microservice.bittorentservice.business.files;

import java.io.IOException;

public interface IBlockReader {
    byte[] readBlock(int index, int begin, int length) throws IOException;
}
