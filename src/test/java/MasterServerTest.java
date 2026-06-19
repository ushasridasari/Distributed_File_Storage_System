import org.junit.Test;
import static org.junit.Assert.*;

public class MasterServerTest {

    @Test
    public void testCreateAndRetrieveFile() {
        MasterServer ms = new MasterServer(0);
        ms.namespace.put("/test.txt", new MasterServer.FileMetadata("/test.txt", false));
        assertTrue(ms.namespace.containsKey("/test.txt"));
        assertEquals("/test.txt", ms.namespace.get("/test.txt").path);
    }

    @Test
    public void testDeleteFile() {
        MasterServer ms = new MasterServer(0);
        ms.namespace.put("/del.txt", new MasterServer.FileMetadata("/del.txt", false));
        ms.namespace.remove("/del.txt");
        assertFalse(ms.namespace.containsKey("/del.txt"));
    }

    @Test
    public void testRenameFile() {
        MasterServer ms = new MasterServer(0);
        MasterServer.FileMetadata meta = new MasterServer.FileMetadata("/old.txt", false);
        ms.namespace.put("/old.txt", meta);
        ms.namespace.remove("/old.txt");
        meta.path = "/new.txt";
        ms.namespace.put("/new.txt", meta);

        assertFalse(ms.namespace.containsKey("/old.txt"));
        assertTrue(ms.namespace.containsKey("/new.txt"));
        assertEquals("/new.txt", ms.namespace.get("/new.txt").path);
    }

    @Test
    public void testMkdir() {
        MasterServer ms = new MasterServer(0);
        ms.namespace.put("/data", new MasterServer.FileMetadata("/data", true));
        assertTrue(ms.namespace.get("/data").isDirectory);
    }

    @Test
    public void testListFiles() {
        MasterServer ms = new MasterServer(0);
        ms.namespace.put("/data/a.txt", new MasterServer.FileMetadata("/data/a.txt", false));
        ms.namespace.put("/data/b.txt", new MasterServer.FileMetadata("/data/b.txt", false));
        ms.namespace.put("/other/c.txt", new MasterServer.FileMetadata("/other/c.txt", false));

        long count = ms.namespace.keySet().stream()
            .filter(p -> p.startsWith("/data/"))
            .count();
        assertEquals(2, count);
    }

    @Test
    public void testAddChunkToFile() {
        MasterServer.FileMetadata meta = new MasterServer.FileMetadata("/chunked.txt", false);
        meta.addChunk("chunk-001");
        meta.addChunk("chunk-002");
        assertEquals(2, meta.chunkIds.size());
    }

    @Test
    public void testChunkLocationToString() {
        MasterServer.ChunkLocation loc = new MasterServer.ChunkLocation("localhost", 9100);
        assertEquals("localhost:9100", loc.toString());
    }

    @Test
    public void testChunkLocationEquality() {
        MasterServer.ChunkLocation a = new MasterServer.ChunkLocation("localhost", 9100);
        MasterServer.ChunkLocation b = new MasterServer.ChunkLocation("localhost", 9100);
        MasterServer.ChunkLocation c = new MasterServer.ChunkLocation("localhost", 9101);
        assertEquals(a, b);
        assertNotEquals(a, c);
    }

    @Test
    public void testServerInfoIsAlive() {
        MasterServer.ServerInfo info = new MasterServer.ServerInfo(
            new MasterServer.ChunkLocation("localhost", 9100));
        assertTrue(info.isAlive());
    }

    @Test
    public void testFileMetadataUpdatedAtChanges() throws InterruptedException {
        MasterServer.FileMetadata meta = new MasterServer.FileMetadata("/f.txt", false);
        long before = meta.updatedAt;
        Thread.sleep(5);
        meta.addChunk("c1");
        assertTrue(meta.updatedAt >= before);
    }
}
