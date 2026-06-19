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
    public void testListChunks() throws IOException {
        storage.write("c1", "a".getBytes());
        storage.write("c2", "b".getBytes());
        assertEquals(2, storage.list().size());
    }

    @Test(expected = java.io.FileNotFoundException.class)
    public void testReadMissingChunkThrows() throws IOException {
        storage.read("nonexistent");
    }
}
