import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

/**
 * GFS Master Server — single point of metadata coordination.
 *
 * Mirrors how GitRepository.java centralises all ref/HEAD logic in the
 * reference DVCS project. All master-side concerns live here:
 *   - File namespace (path -> chunk list)
 *   - Chunk server registry (heartbeat tracking + placement)
 *   - Request handler (one connection = one thread)
 *
 * GFS paper responsibilities:
 *   - Maintain file namespace and chunk-to-location mapping
 *   - Grant chunk leases (primary election for writes)
 *   - Direct chunk placement based on server load
 *   - Process heartbeats to detect dead servers
 */
public class MasterServer {

    // -------------------------------------------------------------------------
    // Inner model classes (like IndexEntry inside Index.java in reference repo)
    // -------------------------------------------------------------------------

    static class FileMetadata implements Serializable {
        String path;
        List<String> chunkIds = new ArrayList<>();
        long createdAt = System.currentTimeMillis();
        long fileSize;

        FileMetadata(String path) { this.path = path; }

        void addChunk(String chunkId) { chunkIds.add(chunkId); }
    }

    static class ChunkMetadata implements Serializable {
        String chunkId;
        long version;
        List<ChunkLocation> locations = new ArrayList<>();
        ChunkLocation primary;

        ChunkMetadata(String chunkId, long version) {
            this.chunkId = chunkId;
            this.version = version;
        }
    }

    static class ChunkLocation implements Serializable {
        String host;
        int port;

        ChunkLocation(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override public String toString() { return host + ":" + port; }
    }

    static class ServerInfo {
        ChunkLocation location;
        volatile long lastHeartbeat = System.currentTimeMillis();
        Set<String> chunks = ConcurrentHashMap.newKeySet();

        ServerInfo(ChunkLocation loc) { this.location = loc; }
    }

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    private final int port;
    private final Map<String, FileMetadata>  namespace  = new ConcurrentHashMap<>();
    private final Map<String, ChunkMetadata> chunkTable = new ConcurrentHashMap<>();
    private final Map<String, ServerInfo>    servers    = new ConcurrentHashMap<>();
    private final ExecutorService pool = Executors.newCachedThreadPool();
    private volatile boolean running;

    public MasterServer(int port) { this.port = port; }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    public void start() throws IOException {
        running = true;
        System.out.println("[Master] Listening on port " + port);
        try (ServerSocket ss = new ServerSocket(port)) {
            while (running) {
                Socket client = ss.accept();
                pool.submit(() -> handle(client));
            }
        }
    }

    public void stop() { running = false; pool.shutdown(); }

    // -------------------------------------------------------------------------
    // Request dispatch
    // -------------------------------------------------------------------------

    private void handle(Socket socket) {
        try (ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream  in  = new ObjectInputStream(socket.getInputStream())) {

            Message req  = (Message) in.readObject();
            Message resp = dispatch(req);
            out.writeObject(resp);

        } catch (Exception e) {
            System.err.println("[Master] Handler error: " + e.getMessage());
        } finally {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    private Message dispatch(Message req) {
        switch (req.type) {
            case "CREATE_FILE":           return createFile(req);
            case "DELETE_FILE":           return deleteFile(req);
            case "LIST_FILES":            return listFiles(req);
            case "GET_FILE_INFO":         return getFileInfo(req);
            case "REQUEST_CHUNK_WRITE":   return requestChunkWrite(req);
            case "REQUEST_CHUNK_READ":    return requestChunkRead(req);
            case "REGISTER_CHUNK_SERVER": return registerServer(req);
            case "HEARTBEAT":             return heartbeat(req);
            default:
                return Message.error("Unknown command: " + req.type);
        }
    }

    // -------------------------------------------------------------------------
    // Handlers
    // -------------------------------------------------------------------------

    private Message createFile(Message req) {
        String path = (String) req.get("path");
        if (namespace.containsKey(path))
            return Message.error("File already exists: " + path);
        namespace.put(path, new FileMetadata(path));
        return Message.ok().put("path", path);
    }

    private Message deleteFile(Message req) {
        String path = (String) req.get("path");
        FileMetadata meta = namespace.remove(path);
        if (meta == null) return Message.error("File not found: " + path);
        meta.chunkIds.forEach(chunkTable::remove); // lazy GC
        return Message.ok();
    }

    private Message listFiles(Message req) {
        String prefix = req.has("prefix") ? (String) req.get("prefix") : "/";
        List<String> files = namespace.keySet().stream()
            .filter(p -> p.startsWith(prefix))
            .sorted()
            .collect(Collectors.toList());
        return new Message("FILE_LIST").put("files", files);
    }

    private Message getFileInfo(Message req) {
        String path = (String) req.get("path");
        FileMetadata meta = namespace.get(path);
        if (meta == null) return Message.error("File not found: " + path);
        return new Message("FILE_INFO")
            .put("path", meta.path)
            .put("chunkIds", meta.chunkIds)
            .put("fileSize", meta.fileSize)
            .put("createdAt", meta.createdAt);
    }

    private Message requestChunkWrite(Message req) {
        String path = (String) req.get("path");
        FileMetadata meta = namespace.get(path);
        if (meta == null) return Message.error("File not found: " + path);

        List<ChunkLocation> candidates = selectServers(
            Math.min(GfsConfig.REPLICATION_FACTOR, liveServers().size()));
        if (candidates.isEmpty()) return Message.error("No chunk servers available");

        String chunkId = UUID.randomUUID().toString();
        ChunkMetadata cm = new ChunkMetadata(chunkId, 1L);
        candidates.forEach(loc -> cm.locations.add(loc));
        cm.primary = candidates.get(0);
        chunkTable.put(chunkId, cm);
        meta.addChunk(chunkId);

        return new Message("CHUNK_INFO")
            .put("chunkId", chunkId)
            .put("primary", cm.primary)
            .put("secondaries", candidates.subList(1, candidates.size()));
    }

    private Message requestChunkRead(Message req) {
        String chunkId = (String) req.get("chunkId");
        ChunkMetadata cm = chunkTable.get(chunkId);
        if (cm == null) return Message.error("Chunk not found: " + chunkId);
        return new Message("CHUNK_INFO")
            .put("chunkId", chunkId)
            .put("locations", cm.locations);
    }

    private Message registerServer(Message req) {
        String host = (String) req.get("host");
        int    port = (int)    req.get("port");
        Set<String> chunks = req.has("chunks") ? (Set<String>) req.get("chunks") : new HashSet<>();
        String key = host + ":" + port;
        ServerInfo info = new ServerInfo(new ChunkLocation(host, port));
        info.chunks.addAll(chunks);
        servers.put(key, info);
        System.out.println("[Master] Registered chunk server " + key);
        return Message.ok();
    }

    private Message heartbeat(Message req) {
        String host = (String) req.get("host");
        int    port = (int)    req.get("port");
        Set<String> chunks = req.has("chunks") ? (Set<String>) req.get("chunks") : new HashSet<>();
        String key = host + ":" + port;
        servers.computeIfAbsent(key, k -> new ServerInfo(new ChunkLocation(host, port)));
        ServerInfo info = servers.get(key);
        info.lastHeartbeat = System.currentTimeMillis();
        info.chunks.clear();
        info.chunks.addAll(chunks);
        return Message.ok();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private List<ServerInfo> liveServers() {
        long now = System.currentTimeMillis();
        return servers.values().stream()
            .filter(s -> (now - s.lastHeartbeat) < GfsConfig.CHUNK_SERVER_TIMEOUT_MS)
            .collect(Collectors.toList());
    }

    // Selects least-loaded servers for new chunk placement
    private List<ChunkLocation> selectServers(int count) {
        return liveServers().stream()
            .sorted(Comparator.comparingInt(s -> s.chunks.size()))
            .limit(count)
            .map(s -> s.location)
            .collect(Collectors.toList());
    }

    // -------------------------------------------------------------------------
    // Entry point
    // -------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : GfsConfig.MASTER_PORT;
        MasterServer master = new MasterServer(port);
        Runtime.getRuntime().addShutdownHook(new Thread(master::stop));
        master.start();
    }
}
