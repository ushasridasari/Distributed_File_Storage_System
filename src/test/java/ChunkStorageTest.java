import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

import static org.junit.Assert.*;

public class ChunkStorageTest {

    private Path tempDir;
    private ChunkServer.ChunkStorage storage;

    @Before
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("gfs-test-");
        storage = new ChunkServer.ChunkStorage(tempDir.toString());
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
        storage.write("chunk-001", data);
        assertTrue(storage.has("chunk-001"));
        assertArrayEquals(data, storage.read("chunk-001"));
    }

    @Test
    public void testDelete() throws IOException {
        storage.write("chunk-del", "data".getBytes());
        storage.delete("chunk-del");
        assertFalse(storage.has("chunk-del"));
    }

    @Test
    public void testListChunksExcludesCrcFiles() throws IOException {
        storage.write("c1", "a".getBytes());
        storage.write("c2", "b".getBytes());
        assertEquals(2, storage.list().size()); // .crc sidecars must not be counted
    }

    @Test(expected = java.io.FileNotFoundException.class)
    public void testReadMissingChunkThrows() throws IOException {
        storage.read("nonexistent");
    }

    @Test
    public void testAppendToExistingChunk() throws IOException {
        storage.write("append-chunk", "hello".getBytes());
        storage.append("append-chunk", " world".getBytes());
        assertEquals("hello world", new String(storage.read("append-chunk")));
    }

    @Test
    public void testAppendToNewChunk() throws IOException {
        storage.append("new-chunk", "first".getBytes());
        storage.append("new-chunk", " second".getBytes());
        assertEquals("first second", new String(storage.read("new-chunk")));
    }

    @Test
    public void testCrcSidecarCreatedOnWrite() throws IOException {
        storage.write("sidecar-chunk", "data".getBytes());
        assertTrue(Files.exists(tempDir.resolve("sidecar-chunk.crc")));
    }

    @Test
    public void testCrcVerificationPassesOnValidData() throws IOException {
        storage.write("crc-chunk", "valid data".getBytes());
        assertArrayEquals("valid data".getBytes(), storage.read("crc-chunk"));
    }

    @Test
    public void testChunkSize() throws IOException {
        byte[] data = new byte[1024];
        storage.write("sized-chunk", data);
        assertEquals(1024, storage.size("sized-chunk"));
    }

    @Test
    public void testDeleteRemovesCrcSidecar() throws IOException {
        storage.write("to-delete", "data".getBytes());
        storage.delete("to-delete");
        assertFalse(Files.exists(tempDir.resolve("to-delete")));
        assertFalse(Files.exists(tempDir.resolve("to-delete.crc")));
    }
}
