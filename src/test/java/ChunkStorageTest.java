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
        storage.write("chunk-001", data, 1L);
        assertTrue(storage.has("chunk-001"));
        assertArrayEquals(data, storage.read("chunk-001"));
    }

    @Test
    public void testDelete() throws IOException {
        storage.write("chunk-del", "data".getBytes(), 1L);
        storage.delete("chunk-del");
        assertFalse(storage.has("chunk-del"));
    }

    @Test
    public void testDeleteRemovesAllSidecars() throws IOException {
        storage.write("sidecar-del", "data".getBytes(), 2L);
        storage.delete("sidecar-del");
        assertFalse(Files.exists(tempDir.resolve("sidecar-del")));
        assertFalse(Files.exists(tempDir.resolve("sidecar-del.crc")));
        assertFalse(Files.exists(tempDir.resolve("sidecar-del.ver")));
    }

    @Test
    public void testListChunksExcludesSidecars() throws IOException {
        storage.write("c1", "a".getBytes(), 1L);
        storage.write("c2", "b".getBytes(), 1L);
        assertEquals(2, storage.list().size());
    }

    @Test(expected = java.io.FileNotFoundException.class)
    public void testReadMissingChunkThrows() throws IOException {
        storage.read("nonexistent");
    }

    @Test
    public void testAppendToExistingChunk() throws IOException {
        storage.write("append-chunk", "hello".getBytes(), 1L);
        storage.append("append-chunk", " world".getBytes(), 1L);
        assertEquals("hello world", new String(storage.read("append-chunk")));
    }

    @Test
    public void testAppendToNewChunk() throws IOException {
        storage.append("new-chunk", "first".getBytes(), 1L);
        storage.append("new-chunk", " second".getBytes(), 1L);
        assertEquals("first second", new String(storage.read("new-chunk")));
    }

    @Test
    public void testCrcSidecarCreatedOnWrite() throws IOException {
        storage.write("crc-chunk", "data".getBytes(), 1L);
        assertTrue(Files.exists(tempDir.resolve("crc-chunk.crc")));
    }

    @Test
    public void testCrcVerificationPassesOnValidData() throws IOException {
        storage.write("valid-crc", "valid data".getBytes(), 1L);
        assertArrayEquals("valid data".getBytes(), storage.read("valid-crc"));
    }

    // Version sidecar tests
    @Test
    public void testVersionSidecarCreatedOnWrite() throws IOException {
        storage.write("ver-chunk", "data".getBytes(), 3L);
        assertTrue(Files.exists(tempDir.resolve("ver-chunk.ver")));
        assertEquals(3L, storage.readVersion("ver-chunk"));
    }

    @Test
    public void testVersionReportedInListWithVersions() throws IOException {
        storage.write("v1-chunk", "data".getBytes(), 5L);
        storage.write("v2-chunk", "data".getBytes(), 7L);
        var versions = storage.listWithVersions();
        assertEquals(2, versions.size());
        assertEquals(5L, (long) versions.get("v1-chunk"));
        assertEquals(7L, (long) versions.get("v2-chunk"));
    }

    @Test
    public void testVersionUpdatedOnNewWrite() throws IOException {
        storage.write("ver-update", "v1".getBytes(), 1L);
        storage.write("ver-update", "v2".getBytes(), 2L);
        assertEquals(2L, storage.readVersion("ver-update"));
    }

    @Test
    public void testChunkSize() throws IOException {
        byte[] data = new byte[1024];
        storage.write("sized-chunk", data, 1L);
        assertEquals(1024, storage.size("sized-chunk"));
    }
}
