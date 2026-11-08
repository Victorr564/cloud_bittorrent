package com.microservice.bittorentservice.business.files;

import java.nio.file.Path;

/** The part of a block that lives inside one file: which file, where to start in it, how many bytes. */
public record FileSegment(Path path,
                          long fileOffset,
                          long length) {

}