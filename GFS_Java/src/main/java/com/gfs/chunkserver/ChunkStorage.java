package com.gfs.chunkserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.HashSet;
import java.util.Set;

/**
 * Manages raw chunk data on local disk. Each chunk is stored as a flat file named by its chunk ID.
 */
public class ChunkStorage {
    private static final Logger log = LoggerFactory.getLogger(ChunkStorage.class);

    private final Path storageDir;

    public ChunkStorage(String storageDir) throws IOException {
        this.storageDir = Paths.get(storageDir);
        Files.createDirectories(this.storageDir);
    }

    public void writeChunk(String chunkId, byte[] data) throws IOException {
        Path chunkFile = storageDir.resolve(chunkId);
        Files.write(chunkFile, data, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        log.info("Wrote chunk {} ({} bytes)", chunkId, data.length);
    }

    public byte[] readChunk(String chunkId) throws IOException {
        Path chunkFile = storageDir.resolve(chunkId);
        if (!Files.exists(chunkFile)) {
            throw new FileNotFoundException("Chunk not found: " + chunkId);
        }
        return Files.readAllBytes(chunkFile);
    }

    public void deleteChunk(String chunkId) throws IOException {
        Path chunkFile = storageDir.resolve(chunkId);
        Files.deleteIfExists(chunkFile);
        log.info("Deleted chunk {}", chunkId);
    }

    public boolean hasChunk(String chunkId) {
        return Files.exists(storageDir.resolve(chunkId));
    }

    public Set<String> listChunks() throws IOException {
        Set<String> chunks = new HashSet<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(storageDir)) {
            for (Path entry : stream) {
                chunks.add(entry.getFileName().toString());
            }
        }
        return chunks;
    }

    public long getChunkSize(String chunkId) throws IOException {
        return Files.size(storageDir.resolve(chunkId));
    }
}
