package com.gfs;

import com.gfs.chunkserver.ChunkStorage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

import static org.junit.Assert.*;

public class ChunkStorageTest {
    private Path tempDir;
    private ChunkStorage storage;

    @Before
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("gfs-test-");
        storage = new ChunkStorage(tempDir.toString());
    }

    @After
    public void tearDown() throws IOException {
        Files.walkFileTree(tempDir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }
            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    @Test
    public void testWriteAndRead() throws IOException {
        byte[] data = "hello gfs".getBytes();
        storage.writeChunk("chunk-001", data);
        assertTrue(storage.hasChunk("chunk-001"));
        assertArrayEquals(data, storage.readChunk("chunk-001"));
    }

    @Test
    public void testDelete() throws IOException {
        storage.writeChunk("chunk-del", "data".getBytes());
        storage.deleteChunk("chunk-del");
        assertFalse(storage.hasChunk("chunk-del"));
    }

    @Test
    public void testListChunks() throws IOException {
        storage.writeChunk("c1", "a".getBytes());
        storage.writeChunk("c2", "b".getBytes());
        assertEquals(2, storage.listChunks().size());
    }

    @Test(expected = java.io.FileNotFoundException.class)
    public void testReadMissingChunkThrows() throws IOException {
        storage.readChunk("nonexistent");
    }
}
