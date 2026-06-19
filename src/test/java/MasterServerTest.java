import org.junit.Test;
import static org.junit.Assert.*;
import java.util.List;

public class MasterServerTest {

    @Test
    public void testCreateAndRetrieveFile() {
        MasterServer ms = new MasterServer(0);
        ms.namespace.put("/test.txt", new MasterServer.FileMetadata("/test.txt"));
        assertTrue(ms.namespace.containsKey("/test.txt"));
        assertEquals("/test.txt", ms.namespace.get("/test.txt").path);
    }

    @Test
    public void testDeleteFile() {
        MasterServer ms = new MasterServer(0);
        ms.namespace.put("/del.txt", new MasterServer.FileMetadata("/del.txt"));
        ms.namespace.remove("/del.txt");
        assertFalse(ms.namespace.containsKey("/del.txt"));
    }

    @Test
    public void testListFiles() {
        MasterServer ms = new MasterServer(0);
        ms.namespace.put("/data/a.txt", new MasterServer.FileMetadata("/data/a.txt"));
        ms.namespace.put("/data/b.txt", new MasterServer.FileMetadata("/data/b.txt"));
        ms.namespace.put("/other/c.txt", new MasterServer.FileMetadata("/other/c.txt"));

        long count = ms.namespace.keySet().stream()
            .filter(p -> p.startsWith("/data/"))
            .count();
        assertEquals(2, count);
    }

    @Test
    public void testAddChunkToFile() {
        MasterServer.FileMetadata meta = new MasterServer.FileMetadata("/chunked.txt");
        meta.addChunk("chunk-001");
        meta.addChunk("chunk-002");
        assertEquals(2, meta.chunkIds.size());
    }

    @Test
    public void testChunkLocationToString() {
        MasterServer.ChunkLocation loc = new MasterServer.ChunkLocation("localhost", 9100);
        assertEquals("localhost:9100", loc.toString());
    }
}
