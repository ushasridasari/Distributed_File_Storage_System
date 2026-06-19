import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

/**
 * GFS Master Server — single point of metadata coordination.
 *
 * Mirrors how GitRepository.java centralises all ref/HEAD logic in the
 * reference DVCS project. All master-side concerns live here:
 *   - File namespace (path -> chunk list) with directory support
 *   - Chunk server registry (heartbeat tracking + placement)
 *   - Request handler (one connection = one thread)
 *   - Operation log for crash recovery (like Git's reflog)
 *   - Background re-replication when servers go offline
 *
 * GFS paper responsibilities:
 *   - Maintain file namespace and chunk-to-location mapping
 *   - Grant chunk leases (primary election for writes)
 *   - Direct chunk placement based on server load
 *   - Process heartbeats to detect dead servers
 *   - Re-replicate chunks that fall below replication factor
 */
public class MasterServer {

    // -------------------------------------------------------------------------
    // Inner model classes (like IndexEntry inside Index.java in reference repo)
    // -------------------------------------------------------------------------

    static class FileMetadata implements Serializable {
        String path;
        List<String> chunkIds = new ArrayList<>();
        long createdAt = System.currentTimeMillis();
        long updatedAt = createdAt;
        long fileSize;
        boolean isDirectory;

        FileMetadata(String path, boolean isDirectory) {
            this.path = path;
            this.isDirectory = isDirectory;
        }

        void addChunk(String chunkId) {
            chunkIds.add(chunkId);
            updatedAt = System.currentTimeMillis();
        }
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

        @Override public boolean equals(Object o) {
            if (!(o instanceof ChunkLocation)) return false;
            ChunkLocation c = (ChunkLocation) o;
            return port == c.port && host.equals(c.host);
        }

        @Override public int hashCode() { return Objects.hash(host, port); }
    }

    static class ServerInfo {
        ChunkLocation location;
        volatile long lastHeartbeat = System.currentTimeMillis();
        Set<String> chunks = ConcurrentHashMap.newKeySet();

        ServerInfo(ChunkLocation loc) { this.location = loc; }

        boolean isAlive() {
            return (System.currentTimeMillis() - lastHeartbeat) < GfsConfig.CHUNK_SERVER_TIMEOUT_MS;
        }
    }

    // -------------------------------------------------------------------------
    // Operation log — mirrors Git's reflog for crash recovery
    // -------------------------------------------------------------------------

    static class OperationLog {
        private final Path logFile;

        OperationLog(String logPath) throws IOException {
            this.logFile = Paths.get(logPath);
            Files.createDirectories(logFile.getParent());
        }

        void append(String operation, String details) {
            try {
                String entry = String.format("[%s] %s %s%n",
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()),
                    operation, details);
                Files.writeString(logFile, entry,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                System.err.println("[Master] Failed to write operation log: " + e.getMessage());
            }
        }

        List<String> readAll() throws IOException {
            if (!Files.exists(logFile)) return new ArrayList<>();
            return Files.readAllLines(logFile);
        }
    }

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    private final int port;
    private final Map<String, FileMetadata>  namespace  = new ConcurrentHashMap<>();
    private final Map<String, ChunkMetadata> chunkTable = new ConcurrentHashMap<>();
    private final Map<String, ServerInfo>    servers    = new ConcurrentHashMap<>();
    private final ExecutorService pool      = Executors.newCachedThreadPool();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private OperationLog opLog;
    private volatile boolean running;

    public MasterServer(int port) { this.port = port; }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    public void start() throws IOException {
        running = true;
        opLog = new OperationLog(".gfs/master.log");
        scheduleReReplication();
        System.out.println("[Master] Listening on port " + port);
        try (ServerSocket ss = new ServerSocket(port)) {
            while (running) {
                Socket client = ss.accept();
                pool.submit(() -> handle(client));
            }
        }
    }

    public void stop() {
        running = false;
        pool.shutdown();
        scheduler.shutdown();
    }

    // -------------------------------------------------------------------------
    // Background re-replication (GFS §4.4)
    // Detects under-replicated chunks after server failures and re-replicates
    // -------------------------------------------------------------------------

    private void scheduleReReplication() {
        scheduler.scheduleAtFixedRate(() -> {
            for (Map.Entry<String, ChunkMetadata> entry : chunkTable.entrySet()) {
                ChunkMetadata cm = entry.getValue();
                long liveReplicas = cm.locations.stream()
                    .filter(loc -> {
                        ServerInfo si = servers.get(loc.toString());
                        return si != null && si.isAlive();
                    }).count();

                if (liveReplicas < GfsConfig.REPLICATION_FACTOR && liveReplicas > 0) {
                    triggerReReplication(cm, (int) liveReplicas);
                }
            }
        }, 10000, 10000, TimeUnit.MILLISECONDS);
    }

    private void triggerReReplication(ChunkMetadata cm, int currentReplicas) {
        int needed = GfsConfig.REPLICATION_FACTOR - currentReplicas;
        List<ChunkLocation> liveLocations = cm.locations.stream()
            .filter(loc -> { ServerInfo si = servers.get(loc.toString()); return si != null && si.isAlive(); })
            .collect(Collectors.toList());

        if (liveLocations.isEmpty()) return;

        List<ChunkLocation> newTargets = liveServers().stream()
            .map(s -> s.location)
            .filter(loc -> !cm.locations.contains(loc))
            .limit(needed)
            .collect(Collectors.toList());

        ChunkLocation source = liveLocations.get(0);
        for (ChunkLocation target : newTargets) {
            try (Socket s = new Socket(source.host, source.port);
                 ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                 ObjectInputStream  in  = new ObjectInputStream(s.getInputStream())) {

                out.writeObject(new Message("READ_CHUNK").put("chunkId", cm.chunkId));
                Message resp = (Message) in.readObject();
                if ("OK".equals(resp.type)) {
                    byte[] data = (byte[]) resp.get("data");
                    try (Socket t   = new Socket(target.host, target.port);
                         ObjectOutputStream tout = new ObjectOutputStream(t.getOutputStream());
                         ObjectInputStream  tin  = new ObjectInputStream(t.getInputStream())) {
                        tout.writeObject(new Message("REPLICATE_CHUNK")
                            .put("chunkId", cm.chunkId).put("data", data));
                        tin.readObject();
                        cm.locations.add(target);
                        System.out.printf("[Master] Re-replicated chunk %s to %s%n", cm.chunkId, target);
                        opLog.append("RE_REPLICATE", cm.chunkId + " -> " + target);
                    }
                }
            } catch (Exception e) {
                System.err.println("[Master] Re-replication failed: " + e.getMessage());
            }
        }
    }

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
            case "RENAME_FILE":           return renameFile(req);
            case "MKDIR":                 return mkdir(req);
            case "LIST_FILES":            return listFiles(req);
            case "GET_FILE_INFO":         return getFileInfo(req);
            case "STAT":                  return stat(req);
            case "REQUEST_CHUNK_WRITE":   return requestChunkWrite(req);
            case "REQUEST_CHUNK_APPEND":  return requestChunkAppend(req);
            case "REQUEST_CHUNK_READ":    return requestChunkRead(req);
            case "REGISTER_CHUNK_SERVER": return registerServer(req);
            case "HEARTBEAT":             return heartbeat(req);
            case "CLUSTER_STATUS":        return clusterStatus(req);
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
        namespace.put(path, new FileMetadata(path, false));
        opLog.append("CREATE", path);
        return Message.ok().put("path", path);
    }

    private Message deleteFile(Message req) {
        String path = (String) req.get("path");
        FileMetadata meta = namespace.remove(path);
        if (meta == null) return Message.error("File not found: " + path);
        meta.chunkIds.forEach(chunkTable::remove); // lazy GC
        opLog.append("DELETE", path);
        return Message.ok();
    }

    private Message renameFile(Message req) {
        String src = (String) req.get("src");
        String dst = (String) req.get("dst");
        if (!namespace.containsKey(src)) return Message.error("File not found: " + src);
        if (namespace.containsKey(dst))  return Message.error("Destination already exists: " + dst);
        FileMetadata meta = namespace.remove(src);
        meta.path = dst;
        meta.updatedAt = System.currentTimeMillis();
        namespace.put(dst, meta);
        opLog.append("RENAME", src + " -> " + dst);
        return Message.ok();
    }

    private Message mkdir(Message req) {
        String path = (String) req.get("path");
        if (namespace.containsKey(path)) return Message.error("Path already exists: " + path);
        namespace.put(path, new FileMetadata(path, true));
        opLog.append("MKDIR", path);
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
            .put("createdAt", meta.createdAt)
            .put("updatedAt", meta.updatedAt)
            .put("isDirectory", meta.isDirectory);
    }

    private Message stat(Message req) {
        String path = (String) req.get("path");
        FileMetadata meta = namespace.get(path);
        if (meta == null) return Message.error("No such file or directory: " + path);

        int totalReplicas = meta.chunkIds.stream()
            .mapToInt(id -> chunkTable.containsKey(id) ? chunkTable.get(id).locations.size() : 0)
            .sum();

        return new Message("STAT")
            .put("path", meta.path)
            .put("isDirectory", meta.isDirectory)
            .put("fileSize", meta.fileSize)
            .put("chunkCount", meta.chunkIds.size())
            .put("totalReplicas", totalReplicas)
            .put("createdAt", meta.createdAt)
            .put("updatedAt", meta.updatedAt);
    }

    private Message requestChunkWrite(Message req) {
        String path = (String) req.get("path");
        FileMetadata meta = namespace.get(path);
        if (meta == null) return Message.error("File not found: " + path);
        return allocateChunk(meta);
    }

    // Atomic record append — GFS §3.3: master picks the last chunk's primary
    private Message requestChunkAppend(Message req) {
        String path = (String) req.get("path");
        FileMetadata meta = namespace.get(path);
        if (meta == null) return Message.error("File not found: " + path);

        // reuse last chunk if it exists, otherwise allocate a new one
        if (!meta.chunkIds.isEmpty()) {
            String lastChunkId = meta.chunkIds.get(meta.chunkIds.size() - 1);
            ChunkMetadata cm = chunkTable.get(lastChunkId);
            if (cm != null && !cm.locations.isEmpty()) {
                return new Message("CHUNK_INFO")
                    .put("chunkId", cm.chunkId)
                    .put("primary", cm.primary)
                    .put("secondaries", cm.locations.subList(1, cm.locations.size()))
                    .put("append", true);
            }
        }
        return allocateChunk(meta);
    }

    private Message allocateChunk(FileMetadata meta) {
        List<ChunkLocation> candidates = selectServers(
            Math.min(GfsConfig.REPLICATION_FACTOR, liveServers().size()));
        if (candidates.isEmpty()) return Message.error("No chunk servers available");

        String chunkId = UUID.randomUUID().toString();
        ChunkMetadata cm = new ChunkMetadata(chunkId, 1L);
        candidates.forEach(loc -> cm.locations.add(loc));
        cm.primary = candidates.get(0);
        chunkTable.put(chunkId, cm);
        meta.addChunk(chunkId);
        opLog.append("ALLOC_CHUNK", chunkId + " for " + meta.path);

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
        opLog.append("REGISTER_SERVER", key);
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

    private Message clusterStatus(Message req) {
        List<ServerInfo> live = liveServers();
        List<ServerInfo> dead = servers.values().stream()
            .filter(s -> !s.isAlive()).collect(Collectors.toList());

        int totalChunks = chunkTable.size();
        int totalFiles  = (int) namespace.values().stream().filter(m -> !m.isDirectory).count();
        int totalDirs   = (int) namespace.values().stream().filter(m ->  m.isDirectory).count();

        List<String> liveList = live.stream().map(s -> s.location.toString()).collect(Collectors.toList());
        List<String> deadList = dead.stream().map(s -> s.location.toString()).collect(Collectors.toList());

        return new Message("CLUSTER_STATUS")
            .put("liveServers",  liveList)
            .put("deadServers",  deadList)
            .put("totalChunks",  totalChunks)
            .put("totalFiles",   totalFiles)
            .put("totalDirs",    totalDirs);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private List<ServerInfo> liveServers() {
        return servers.values().stream()
            .filter(ServerInfo::isAlive)
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
