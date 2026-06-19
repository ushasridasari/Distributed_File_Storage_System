import org.junit.Test;
import static org.junit.Assert.*;
import java.util.*;

public class MasterServerTest {

    @Test
    public void testCreateAndRetrieveFile() {
        MasterServer ms = new MasterServer(0);
        ms.namespace.put("/test.txt", new MasterServer.FileMetadata("/test.txt", false));
        assertTrue(ms.namespace.containsKey("/test.txt"));
        assertEquals("/test.txt", ms.namespace.get("/test.txt").path);
    }

    @Test
    public void testDeleteFileRemovesFromNamespace() {
        MasterServer ms = new MasterServer(0);
        ms.namespace.put("/del.txt", new MasterServer.FileMetadata("/del.txt", false));
        ms.namespace.remove("/del.txt");
        assertFalse(ms.namespace.containsKey("/del.txt"));
    }

    @Test
    public void testDeleteFileRemovesChunkMetadata() {
        MasterServer ms = new MasterServer(0);
        MasterServer.FileMetadata meta = new MasterServer.FileMetadata("/f.txt", false);
        meta.addChunk("chunk-1");
        ms.namespace.put("/f.txt", meta);
        ms.chunkTable.put("chunk-1", new MasterServer.ChunkMetadata("chunk-1", 1L));

        // simulate deleteFile logic
        MasterServer.FileMetadata removed = ms.namespace.remove("/f.txt");
        removed.chunkIds.forEach(ms.chunkTable::remove);

        assertFalse(ms.chunkTable.containsKey("chunk-1"));
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
        long count = ms.namespace.keySet().stream().filter(p -> p.startsWith("/data/")).count();
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

    // Chunk version tests
    @Test
    public void testChunkMetadataVersion() {
        MasterServer.ChunkMetadata cm = new MasterServer.ChunkMetadata("c1", 1L);
        assertEquals(1L, cm.version);
    }

    @Test
    public void testLeaseGrantAndValid() {
        MasterServer.ChunkMetadata cm = new MasterServer.ChunkMetadata("c1", 1L);
        MasterServer.ChunkLocation primary = new MasterServer.ChunkLocation("localhost", 9100);
        assertFalse(cm.leaseValid()); // no lease yet
        cm.grantLease(primary);
        assertTrue(cm.leaseValid());
        assertEquals(primary, cm.primary);
    }

    @Test
    public void testLeaseRevoke() {
        MasterServer.ChunkMetadata cm = new MasterServer.ChunkMetadata("c1", 1L);
        cm.grantLease(new MasterServer.ChunkLocation("localhost", 9100));
        cm.revokeLease();
        assertFalse(cm.leaseValid());
        assertNull(cm.primary);
    }

    @Test
    public void testStaleReplicaEviction() {
        MasterServer ms = new MasterServer(0);
        MasterServer.ChunkLocation loc = new MasterServer.ChunkLocation("localhost", 9100);
        MasterServer.ChunkMetadata cm = new MasterServer.ChunkMetadata("chunk-stale", 5L);
        cm.locations.add(loc);
        ms.chunkTable.put("chunk-stale", cm);

        MasterServer.ServerInfo server = new MasterServer.ServerInfo(loc);
        server.chunkVersions.put("chunk-stale", 3L); // stale: reported v3, master expects v5

        // simulate eviction check
        for (Map.Entry<String, Long> entry : server.chunkVersions.entrySet()) {
            MasterServer.ChunkMetadata chunk = ms.chunkTable.get(entry.getKey());
            if (chunk != null && entry.getValue() < chunk.version) {
                chunk.locations.remove(server.location);
            }
        }

        assertFalse(cm.locations.contains(loc));
    }

    @Test
    public void testFileMetadataUpdatedAtChanges() throws InterruptedException {
        MasterServer.FileMetadata meta = new MasterServer.FileMetadata("/f.txt", false);
        long before = meta.updatedAt;
        Thread.sleep(5);
        meta.addChunk("c1");
        assertTrue(meta.updatedAt >= before);
    }

    @Test
    public void testFileSizeSetAfterUpload() {
        MasterServer ms = new MasterServer(0);
        ms.namespace.put("/sized.txt", new MasterServer.FileMetadata("/sized.txt", false));

        // Simulate UPDATE_FILE_SIZE RPC
        MasterServer.FileMetadata meta = ms.namespace.get("/sized.txt");
        meta.fileSize = 134217728L; // 128 MB
        meta.updatedAt = System.currentTimeMillis();

        assertEquals(134217728L, ms.namespace.get("/sized.txt").fileSize);
    }
}
