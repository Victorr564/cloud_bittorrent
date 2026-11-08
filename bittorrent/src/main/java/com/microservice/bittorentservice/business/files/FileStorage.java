package com.microservice.bittorentservice.business.files;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A torrent's pieces cover ONE byte stream: all its files glued together in the order the torrent lists them.
 * This class knows where every file sits in that stream and where it lives on disk, and turns
 * "bytes X..Y of the stream" into the parts of the real files that hold them.
 *
 * Files go directly under the download directory. The torrent's "name" is only a suggestion, so it is not used as a folder.
 * The torrent comes from outside, so every path is validated and must stay inside the download directory.
 * Nothing changes after construction, so every peer thread can use the same instance.
 */
public class FileStorage {
    private final Map<String, List<Long>> offsetMap = new LinkedHashMap<>(); // path -> [first byte, last byte] in the stream
    private final Map<String, Path> locations = new LinkedHashMap<>();       // path -> where it lives on disk
    private final long totalLength;

    public FileStorage(List<Map<String, Object>> files, Path downloadDirectory) {
        if (files == null || files.isEmpty()) {
            throw new IllegalArgumentException("Torrent has no files");
        }

        Path root = downloadDirectory.toAbsolutePath().normalize();
        long currentFileOffset = 0;
        for (Map<String, Object> file : files) {
            long length = parseLength(file);
            String curatedPath = String.join("/", parsePath(file));

            if (offsetMap.containsKey(curatedPath)) {
                // a duplicate would silently overwrite an entry and shift the offset of every file after it
                throw new IllegalArgumentException("Duplicate file in torrent: " + curatedPath);
            }

            Path location = root.resolve(curatedPath).normalize();
            if (!location.startsWith(root) || location.equals(root)) {
                throw new IllegalArgumentException("File path escapes the download directory: " + curatedPath);
            }

            offsetMap.put(curatedPath, List.of(currentFileOffset, currentFileOffset + length - 1));
            locations.put(curatedPath, location);
            currentFileOffset += length;
        }
        this.totalLength = currentFileOffset;
    }

    private static long parseLength(Map<String, Object> file) {
        Object raw = file.get("length");
        if (!(raw instanceof Number)) {
            throw new IllegalArgumentException("File entry without a length: " + file);
        }
        long length = ((Number) raw).longValue();
        if (length < 0) {
            throw new IllegalArgumentException("Negative file length: " + file);
        }
        return length;
    }

    private static List<String> parsePath(Map<String, Object> file) {
        Object raw = file.get("path");
        if (!(raw instanceof List) || ((List<?>) raw).isEmpty()) {
            throw new IllegalArgumentException("File entry without a path: " + file);
        }

        List<String> components = new ArrayList<>();
        for (Object part : (List<?>) raw) {
            String component = String.valueOf(part);
            if (component.isEmpty() || component.equals(".") || component.equals("..")
                    || component.contains("/") || component.contains("\\") || component.indexOf('\0') >= 0) {
                throw new IllegalArgumentException("Unsafe path component in " + file);
            }
            components.add(component);
        }
        return components;
    }

    /** Creates the folders and every file, each at its full declared length (empty files included). Call once, before any read or write. */
    public void createFiles() throws IOException {
        for (Map.Entry<String, Path> entry : locations.entrySet()) {
            List<Long> range = offsetMap.get(entry.getKey());
            long length = range.get(1) - range.get(0) + 1;

            Path location = entry.getValue();
            Files.createDirectories(location.getParent());
            try (RandomAccessFile raf = new RandomAccessFile(location.toFile(), "rw")) {
                if (raf.length() < length) {
                    raf.setLength(length); // extends with zeros, so a block can be written at any offset
                }
            }
        }
    }

    public long totalLength() {
        return totalLength;
    }

    public Map<String, List<Long>> getOffsetMap() {
        return this.offsetMap;
    }

    /** The parts of the files that hold bytes [fileOffset, fileOffset + length) of the stream, in order. */
    public List<FileSegment> select(long fileOffset, long length) throws IOException {
        if (fileOffset < 0 || length < 0 || fileOffset + length > totalLength) {
            throw new IOException("Range outside the torrent: offset=" + fileOffset + " length=" + length);
        }

        List<FileSegment> selected = new ArrayList<>();
        if (length == 0) {
            return selected;
        }

        long requestEnd = fileOffset + length;
        for (Map.Entry<String, List<Long>> entry : offsetMap.entrySet()) {
            long fileStart = entry.getValue().get(0);
            long fileEnd = entry.getValue().get(1) + 1; // exclusive

            long overlapStart = Math.max(fileOffset, fileStart);
            long overlapEnd = Math.min(requestEnd, fileEnd);

            if (overlapStart < overlapEnd) { // an empty file never overlaps anything
                selected.add(new FileSegment(
                        locations.get(entry.getKey()),
                        overlapStart - fileStart,
                        overlapEnd - overlapStart
                ));
            }
        }
        return selected;
    }
}