import java.io.*;
import java.net.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * GFS Client — high-level API for all file operations.
 *
 * Mirrors how the reference DVCS project bundles all client-side workflow
 * (add -> commit -> checkout) in focused helper methods. All client-side
 * concerns live here:
 *   - Talk to Master for metadata (file info, chunk locations, lease grants)
 *   - Talk directly to ChunkServers for data
 *   - Split files into 64 MB chunks on upload; reassemble on download
 *   - Pass chunk version from lease to ChunkServer on every write
 *   - Client-side chunk location cache (60 s TTL) to reduce Master load
 *
 * Operations:
 *   upload, download, append, delete, rename, mkdir, list, stat, clusterStatus
 */
public class GfsClient {

    // -------------------------------------------------------------------------
    // Client-side chunk location cache (TTL-based)
    // -------------------------------------------------------------------------

    private static class CacheEntry {
        List<MasterServer.ChunkLocation> locations;
        long expiresAt;

        CacheEntry(List<MasterServer.ChunkLocation> locations) {
            this.locations = locations;
            this.expiresAt = System.currentTimeMillis() + GfsConfig.CACHE_TTL_MS;
        }

        boolean isValid() { return System.currentTimeMillis() < expiresAt; }
    }

    private final Map<String, CacheEntry> locationCache = new HashMap<>();

    private List<MasterServer.ChunkLocation> getCachedLocations(String chunkId) {
        CacheEntry e = locationCache.get(chunkId);
        return (e != null && e.isValid()) ? e.locations : null;
    }

    private void cacheLocations(String chunkId, List<MasterServer.ChunkLocation> locs) {
        locationCache.put(chunkId, new CacheEntry(locs));
    }

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    private final String masterHost;
    private final int    masterPort;

    public GfsClient() { this(GfsConfig.MASTER_HOST, GfsConfig.MASTER_PORT); }

    public GfsClient(String masterHost, int masterPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    public void upload(String localPath, String gfsPath) throws IOException {
        File file = new File(localPath);
        if (!file.exists()) throw new FileNotFoundException("Local file not found: " + localPath);

        Message createResp = sendToMaster(new Message("CREATE_FILE").put("path", gfsPath));
        if ("ERROR".equals(createResp.type)) throw new IOException(createResp.get("error").toString());

        byte[] fileBytes = readAllBytes(file);
        int totalChunks  = Math.max(1, (int) Math.ceil((double) fileBytes.length / GfsConfig.CHUNK_SIZE_BYTES));

        for (int i = 0; i < totalChunks; i++) {
            int    start     = i * GfsConfig.CHUNK_SIZE_BYTES;
            int    end       = Math.min(start + GfsConfig.CHUNK_SIZE_BYTES, fileBytes.length);
            byte[] chunkData = Arrays.copyOfRange(fileBytes, start, end);

            Message lease = sendToMaster(new Message("REQUEST_CHUNK_WRITE").put("path", gfsPath));
            if ("ERROR".equals(lease.type)) throw new IOException(lease.get("error").toString());

            String chunkId  = (String) lease.get("chunkId");
            long   version  = lease.has("version") ? ((Number) lease.get("version")).longValue() : 1L;
            MasterServer.ChunkLocation primary    = (MasterServer.ChunkLocation) lease.get("primary");
            List<MasterServer.ChunkLocation> secs = getSecondaries(lease);

            System.out.printf("[GfsClient] Uploading chunk %d/%d (id=%s, v%d) -> %s%n",
                i + 1, totalChunks, chunkId, version, primary);
            writeChunk(primary, secs, chunkId, version, chunkData);
        }
        System.out.printf("[GfsClient] Upload complete: %s -> %s (%d chunk(s))%n",
            localPath, gfsPath, totalChunks);
    }

    public void download(String gfsPath, String localPath) throws IOException {
        Message info = sendToMaster(new Message("GET_FILE_INFO").put("path", gfsPath));
        if ("ERROR".equals(info.type)) throw new IOException(info.get("error").toString());

        List<String> chunkIds = (List<String>) info.get("chunkIds");
        System.out.printf("[GfsClient] Downloading %s (%d chunk(s)) -> %s%n",
            gfsPath, chunkIds.size(), localPath);

        try (FileOutputStream fos = new FileOutputStream(localPath)) {
            for (String chunkId : chunkIds) {
                List<MasterServer.ChunkLocation> locations = resolveLocations(chunkId);
                byte[] data = readChunkWithFallback(chunkId, locations);
                fos.write(data);
            }
        }
        System.out.println("[GfsClient] Download complete: " + localPath);
    }

    // Atomic record append (GFS §3.3)
    public long append(String gfsPath, byte[] data) throws IOException {
        Message lease = sendToMaster(new Message("REQUEST_CHUNK_APPEND").put("path", gfsPath));
        if ("ERROR".equals(lease.type)) throw new IOException(lease.get("error").toString());

        String chunkId = (String) lease.get("chunkId");
        long   version = lease.has("version") ? ((Number) lease.get("version")).longValue() : 1L;
        MasterServer.ChunkLocation primary    = (MasterServer.ChunkLocation) lease.get("primary");
        List<MasterServer.ChunkLocation> secs = getSecondaries(lease);

        try (Socket s   = new Socket(primary.host, primary.port);
             ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
             ObjectInputStream  in  = new ObjectInputStream(s.getInputStream())) {

            out.writeObject(new Message("APPEND_CHUNK")
                .put("chunkId", chunkId)
                .put("data", data)
                .put("version", version)
                .put("secondaries", secs));
            Message resp = (Message) in.readObject();
            if ("ERROR".equals(resp.type))
                throw new IOException("Append failed: " + resp.get("error"));
            long offset = resp.has("offset") ? ((Number) resp.get("offset")).longValue() : 0L;
            System.out.printf("[GfsClient] Appended %d bytes to %s at offset %d%n",
                data.length, gfsPath, offset);
            return offset;
        } catch (ClassNotFoundException e) {
            throw new IOException("Deserialization error", e);
        }
    }

    public void deleteFile(String gfsPath) throws IOException {
        Message resp = sendToMaster(new Message("DELETE_FILE").put("path", gfsPath));
        if ("ERROR".equals(resp.type)) throw new IOException(resp.get("error").toString());
        System.out.println("[GfsClient] Deleted: " + gfsPath);
    }

    public void rename(String src, String dst) throws IOException {
        Message resp = sendToMaster(new Message("RENAME_FILE").put("src", src).put("dst", dst));
        if ("ERROR".equals(resp.type)) throw new IOException(resp.get("error").toString());
        System.out.println("[GfsClient] Renamed: " + src + " -> " + dst);
    }

    public void mkdir(String gfsPath) throws IOException {
        Message resp = sendToMaster(new Message("MKDIR").put("path", gfsPath));
        if ("ERROR".equals(resp.type)) throw new IOException(resp.get("error").toString());
        System.out.println("[GfsClient] Created directory: " + gfsPath);
    }

    public List<String> listFiles(String prefix) throws IOException {
        Message resp = sendToMaster(new Message("LIST_FILES").put("prefix", prefix));
        return (List<String>) resp.get("files");
    }

    public void stat(String gfsPath) throws IOException {
        Message resp = sendToMaster(new Message("STAT").put("path", gfsPath));
        if ("ERROR".equals(resp.type)) throw new IOException(resp.get("error").toString());

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println("Path        : " + resp.get("path"));
        System.out.println("Type        : " + (Boolean.TRUE.equals(resp.get("isDirectory")) ? "directory" : "file"));
        System.out.println("Size        : " + resp.get("fileSize") + " bytes");
        System.out.println("Chunks      : " + resp.get("chunkCount"));
        System.out.println("Replicas    : " + resp.get("totalReplicas"));
        System.out.println("Created     : " + sdf.format(new Date(((Number) resp.get("createdAt")).longValue())));
        System.out.println("Modified    : " + sdf.format(new Date(((Number) resp.get("updatedAt")).longValue())));
    }

    public void clusterStatus() throws IOException {
        Message resp = sendToMaster(new Message("CLUSTER_STATUS"));
        if ("ERROR".equals(resp.type)) throw new IOException(resp.get("error").toString());

        List<String> live = (List<String>) resp.get("liveServers");
        List<String> dead = (List<String>) resp.get("deadServers");
        System.out.println("=== GFS Cluster Status ===");
        System.out.println("Live servers (" + live.size() + "): " + live);
        System.out.println("Dead servers (" + dead.size() + "): " + dead);
        System.out.println("Total files  : " + resp.get("totalFiles"));
        System.out.println("Total dirs   : " + resp.get("totalDirs"));
        System.out.println("Total chunks : " + resp.get("totalChunks"));
    }

    // -------------------------------------------------------------------------
    // Chunk-level I/O
    // -------------------------------------------------------------------------

    private void writeChunk(MasterServer.ChunkLocation primary,
                            List<MasterServer.ChunkLocation> secondaries,
                            String chunkId, long version, byte[] data) throws IOException {
        try (Socket s   = new Socket(primary.host, primary.port);
             ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
             ObjectInputStream  in  = new ObjectInputStream(s.getInputStream())) {

            out.writeObject(new Message("WRITE_CHUNK")
                .put("chunkId", chunkId)
                .put("version", version)
                .put("data", data)
                .put("secondaries", secondaries));
            Message resp = (Message) in.readObject();
            if ("ERROR".equals(resp.type))
                throw new IOException("Write failed: " + resp.get("error"));
        } catch (ClassNotFoundException e) {
            throw new IOException("Deserialization error", e);
        }
    }

    private byte[] readChunk(MasterServer.ChunkLocation loc, String chunkId) throws IOException {
        try (Socket s   = new Socket(loc.host, loc.port);
             ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
             ObjectInputStream  in  = new ObjectInputStream(s.getInputStream())) {

            out.writeObject(new Message("READ_CHUNK").put("chunkId", chunkId));
            Message resp = (Message) in.readObject();
            if ("ERROR".equals(resp.type))
                throw new IOException("Read failed: " + resp.get("error"));
            return (byte[]) resp.get("data");
        } catch (ClassNotFoundException e) {
            throw new IOException("Deserialization error", e);
        }
    }

    // Try each replica in order; skip stale/dead ones (GFS §3.1)
    private byte[] readChunkWithFallback(String chunkId,
                                         List<MasterServer.ChunkLocation> locations) throws IOException {
        IOException last = null;
        for (MasterServer.ChunkLocation loc : locations) {
            try {
                return readChunk(loc, chunkId);
            } catch (IOException e) {
                System.err.printf("[GfsClient] Replica %s failed for chunk %s, trying next%n", loc, chunkId);
                last = e;
                locationCache.remove(chunkId); // evict stale cache entry
            }
        }
        throw new IOException("All replicas failed for chunk: " + chunkId, last);
    }

    private List<MasterServer.ChunkLocation> resolveLocations(String chunkId) throws IOException {
        List<MasterServer.ChunkLocation> cached = getCachedLocations(chunkId);
        if (cached != null) return cached;

        Message locResp = sendToMaster(new Message("REQUEST_CHUNK_READ").put("chunkId", chunkId));
        if ("ERROR".equals(locResp.type)) throw new IOException(locResp.get("error").toString());

        List<MasterServer.ChunkLocation> locations =
            (List<MasterServer.ChunkLocation>) locResp.get("locations");
        if (locations == null || locations.isEmpty())
            throw new IOException("No replicas for chunk: " + chunkId);

        cacheLocations(chunkId, locations);
        return locations;
    }

    // -------------------------------------------------------------------------
    // Master RPC helper
    // -------------------------------------------------------------------------

    private Message sendToMaster(Message msg) throws IOException {
        try (Socket s   = new Socket(masterHost, masterPort);
             ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
             ObjectInputStream  in  = new ObjectInputStream(s.getInputStream())) {
            out.writeObject(msg);
            return (Message) in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Deserialization error", e);
        }
    }

    @SuppressWarnings("unchecked")
    private List<MasterServer.ChunkLocation> getSecondaries(Message lease) {
        return lease.has("secondaries")
            ? (List<MasterServer.ChunkLocation>) lease.get("secondaries")
            : new ArrayList<>();
    }

    // -------------------------------------------------------------------------
    // Utility
    // -------------------------------------------------------------------------

    private byte[] readAllBytes(File file) throws IOException {
        try (FileInputStream fis = new FileInputStream(file)) {
            return fis.readAllBytes();
        }
    }
}
