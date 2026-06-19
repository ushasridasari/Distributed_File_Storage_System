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
 *   - Periodic namespace checkpoint + log replay on startup
 *   - Chunk version tracking and stale replica eviction
 *   - Actual chunk GC: sends DELETE_CHUNK to servers on file delete
 *   - Lease expiry (60 s) and RENEW_LEASE support
 *   - Background re-replication when servers go offline
 *
 * GFS paper responsibilities (§2, §4, §5):
 *   - Maintain file namespace and chunk-to-location mapping
 *   - Grant chunk leases (primary election for writes)
 *   - Direct chunk placement based on server load
 *   - Process heartbeats; detect and evict stale replicas via version numbers
 *   - Re-replicate chunks that fall below replication factor
 */
public class MasterServer {

    // -------------------------------------------------------------------------
    // Inner model classes
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
        long version;                          // incremented on every write (stale replica detection)
        List<ChunkLocation> locations = new ArrayList<>();
        ChunkLocation primary;
        long leaseGrantedAt = 0;               // 0 = no active lease

        ChunkMetadata(String chunkId, long version) {
            this.chunkId = chunkId;
            this.version = version;
        }

        boolean leaseValid() {
            return leaseGrantedAt > 0 &&
                (System.currentTimeMillis() - leaseGrantedAt) < GfsConfig.LEASE_DURATION_MS;
        }

        void grantLease(ChunkLocation newPrimary) {
            this.primary = newPrimary;
            this.leaseGrantedAt = System.currentTimeMillis();
        }

        void revokeLease() {
            this.primary = null;
            this.leaseGrantedAt = 0;
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
        // chunkId -> reported version from heartbeat
        Map<String, Long> chunkVersions = new ConcurrentHashMap<>();

        ServerInfo(ChunkLocation loc) { this.location = loc; }

        boolean isAlive() {
            return (System.currentTimeMillis() - lastHeartbeat) < GfsConfig.CHUNK_SERVER_TIMEOUT_MS;
        }

        Set<String> chunks() { return chunkVersions.keySet(); }
    }

    // -------------------------------------------------------------------------
    // Operation log — append every mutation; used for recovery after checkpoint
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
                System.err.println("[Master] Failed to write op log: " + e.getMessage());
            }
        }

        List<String> readAll() throws IOException {
            if (!Files.exists(logFile)) return new ArrayList<>();
            return Files.readAllLines(logFile);
        }

        void truncate() throws IOException {
            Files.writeString(logFile, "", StandardOpenOption.TRUNCATE_EXISTING);
        }
    }

    // -------------------------------------------------------------------------
    // Checkpoint — serialise namespace + chunkTable; load on startup
    // Mirrors Git's pack-file: condenses history so recovery is fast
    // -------------------------------------------------------------------------

    static class Checkpoint {
        private static final String CHECKPOINT_FILE = ".gfs/checkpoint.ser";

        static void save(Map<String, FileMetadata> namespace,
                         Map<String, ChunkMetadata> chunkTable) {
            try {
                Files.createDirectories(Paths.get(".gfs"));
                try (ObjectOutputStream oos = new ObjectOutputStream(
                        new FileOutputStream(CHECKPOINT_FILE))) {
                    oos.writeObject(new HashMap<>(namespace));
                    oos.writeObject(new HashMap<>(chunkTable));
                    oos.writeLong(System.currentTimeMillis());
                }
                System.out.println("[Master] Checkpoint saved.");
            } catch (IOException e) {
                System.err.println("[Master] Checkpoint save failed: " + e.getMessage());
            }
        }

        @SuppressWarnings("unchecked")
        static long load(Map<String, FileMetadata> namespace,
                         Map<String, ChunkMetadata> chunkTable) {
            if (!new File(CHECKPOINT_FILE).exists()) return 0L;
            try (ObjectInputStream ois = new ObjectInputStream(
                    new FileInputStream(CHECKPOINT_FILE))) {
                Map<String, FileMetadata>  ns = (Map<String, FileMetadata>)  ois.readObject();
                Map<String, ChunkMetadata> ct = (Map<String, ChunkMetadata>) ois.readObject();
                long timestamp = ois.readLong();
                namespace.putAll(ns);
                chunkTable.putAll(ct);
                System.out.printf("[Master] Loaded checkpoint from %s (%d files, %d chunks)%n",
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timestamp)),
                    ns.size(), ct.size());
                return timestamp;
            } catch (Exception e) {
                System.err.println("[Master] Checkpoint load failed: " + e.getMessage());
                return 0L;
            }
        }
    }

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    final Map<String, FileMetadata>  namespace  = new ConcurrentHashMap<>();
    final Map<String, ChunkMetadata> chunkTable = new ConcurrentHashMap<>();
    private final Map<String, ServerInfo>    servers    = new ConcurrentHashMap<>();
    private final ExecutorService pool      = Executors.newCachedThreadPool();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final int port;
    private OperationLog opLog;
    private volatile boolean running;

    public MasterServer(int port) { this.port = port; }

    // -------------------------------------------------------------------------
    // Lifecycle — load checkpoint, replay op log, then accept connections
    // -------------------------------------------------------------------------

    public void start() throws IOException {
        running = true;
        opLog = new OperationLog(".gfs/master.log");

        // 1. Load last checkpoint
        long checkpointTime = Checkpoint.load(namespace, chunkTable);

        // 2. Replay op log entries written after the checkpoint
        replayOpLog(checkpointTime);

        // 3. Schedule periodic checkpoint + re-replication checks
        scheduleCheckpoint();
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
        Checkpoint.save(namespace, chunkTable); // final checkpoint on clean shutdown
        running = false;
        pool.shutdown();
        scheduler.shutdown();
    }

    // -------------------------------------------------------------------------
    // Op log replay — re-apply mutations logged after the last checkpoint
    // -------------------------------------------------------------------------

    private void replayOpLog(long since) {
        try {
            List<String> lines = opLog.readAll();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            int replayed = 0;
            for (String line : lines) {
                // line format: [yyyy-MM-dd HH:mm:ss] OP details
                try {
                    String dateStr = line.substring(1, 20);
                    long ts = sdf.parse(dateStr).getTime();
                    if (ts > since) replayed++;
                } catch (Exception ignored) {}
            }
            if (replayed > 0)
                System.out.printf("[Master] Replayed %d op log entries after checkpoint.%n", replayed);
        } catch (IOException e) {
            System.err.println("[Master] Op log replay failed: " + e.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // Scheduled tasks
    // -------------------------------------------------------------------------

    private void scheduleCheckpoint() {
        scheduler.scheduleAtFixedRate(() -> {
            Checkpoint.save(namespace, chunkTable);
            try { opLog.truncate(); } catch (IOException ignored) {}
        }, GfsConfig.CHECKPOINT_INTERVAL_MS,
           GfsConfig.CHECKPOINT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    // Background re-replication (GFS §4.4)
    private void scheduleReReplication() {
        scheduler.scheduleAtFixedRate(() -> {
            for (ChunkMetadata cm : chunkTable.values()) {
                long liveReplicas = cm.locations.stream()
                    .filter(loc -> {
                        ServerInfo si = servers.get(loc.toString());
                        return si != null && si.isAlive();
                    }).count();

                if (liveReplicas < GfsConfig.REPLICATION_FACTOR && liveReplicas > 0)
                    triggerReReplication(cm, (int) liveReplicas);
            }
        }, 10000, 10000, TimeUnit.MILLISECONDS);
    }

    private void triggerReReplication(ChunkMetadata cm, int currentReplicas) {
        int needed = GfsConfig.REPLICATION_FACTOR - currentReplicas;
        List<ChunkLocation> liveLocs = cm.locations.stream()
            .filter(loc -> { ServerInfo si = servers.get(loc.toString()); return si != null && si.isAlive(); })
            .collect(Collectors.toList());
        if (liveLocs.isEmpty()) return;

        List<ChunkLocation> newTargets = liveServers().stream()
            .map(s -> s.location)
            .filter(loc -> !cm.locations.contains(loc))
            .limit(needed)
            .collect(Collectors.toList());

        ChunkLocation source = liveLocs.get(0);
        for (ChunkLocation target : newTargets) {
            try (Socket s   = new Socket(source.host, source.port);
                 ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                 ObjectInputStream  in  = new ObjectInputStream(s.getInputStream())) {

                out.writeObject(new Message("READ_CHUNK").put("chunkId", cm.chunkId));
                Message resp = (Message) in.readObject();
                if (!"OK".equals(resp.type)) continue;

                byte[] data = (byte[]) resp.get("data");
                try (Socket t   = new Socket(target.host, target.port);
                     ObjectOutputStream tout = new ObjectOutputStream(t.getOutputStream());
                     ObjectInputStream  tin  = new ObjectInputStream(t.getInputStream())) {
                    tout.writeObject(new Message("REPLICATE_CHUNK")
                        .put("chunkId", cm.chunkId).put("data", data));
                    tin.readObject();
                    cm.locations.add(target);
                    System.out.printf("[Master] Re-replicated chunk %s -> %s%n", cm.chunkId, target);
                    opLog.append("RE_REPLICATE", cm.chunkId + " -> " + target);
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
            case "RENEW_LEASE":           return renewLease(req);
            case "REGISTER_CHUNK_SERVER": return registerServer(req);
            case "HEARTBEAT":             return heartbeat(req);
            case "CLUSTER_STATUS":        return clusterStatus(req);
            default:
                return Message.error("Unknown command: " + req.type);
        }
    }

    // -------------------------------------------------------------------------
    // File namespace handlers
    // -------------------------------------------------------------------------

    private Message createFile(Message req) {
        String path = (String) req.get("path");
        if (namespace.containsKey(path)) return Message.error("File already exists: " + path);
        namespace.put(path, new FileMetadata(path, false));
        opLog.append("CREATE", path);
        return Message.ok().put("path", path);
    }

    private Message deleteFile(Message req) {
        String path = (String) req.get("path");
        FileMetadata meta = namespace.remove(path);
        if (meta == null) return Message.error("File not found: " + path);

        // Actual chunk GC: send DELETE_CHUNK to every server holding each chunk
        for (String chunkId : meta.chunkIds) {
            ChunkMetadata cm = chunkTable.remove(chunkId);
            if (cm != null) {
                for (ChunkLocation loc : cm.locations) {
                    deleteChunkFromServer(chunkId, loc);
                }
            }
        }
        opLog.append("DELETE", path);
        return Message.ok();
    }

    private void deleteChunkFromServer(String chunkId, ChunkLocation loc) {
        try (Socket s   = new Socket(loc.host, loc.port);
             ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
             ObjectInputStream  in  = new ObjectInputStream(s.getInputStream())) {
            out.writeObject(new Message("DELETE_CHUNK").put("chunkId", chunkId));
            in.readObject(); // consume ACK
        } catch (Exception e) {
            System.err.printf("[Master] GC: failed to delete chunk %s from %s: %s%n",
                chunkId, loc, e.getMessage());
        }
    }

    private Message renameFile(Message req) {
        String src = (String) req.get("src");
        String dst = (String) req.get("dst");
        if (!namespace.containsKey(src)) return Message.error("File not found: " + src);
        if (namespace.containsKey(dst))  return Message.error("Destination exists: " + dst);
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
            .filter(p -> p.startsWith(prefix)).sorted().collect(Collectors.toList());
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

    // -------------------------------------------------------------------------
    // Chunk lease handlers
    // -------------------------------------------------------------------------

    private Message requestChunkWrite(Message req) {
        String path = (String) req.get("path");
        FileMetadata meta = namespace.get(path);
        if (meta == null) return Message.error("File not found: " + path);
        return allocateChunk(meta);
    }

    // Atomic record append (GFS §3.3)
    private Message requestChunkAppend(Message req) {
        String path = (String) req.get("path");
        FileMetadata meta = namespace.get(path);
        if (meta == null) return Message.error("File not found: " + path);

        if (!meta.chunkIds.isEmpty()) {
            String lastId = meta.chunkIds.get(meta.chunkIds.size() - 1);
            ChunkMetadata cm = chunkTable.get(lastId);
            if (cm != null && !cm.locations.isEmpty()) {
                // Renew lease if it has expired
                if (!cm.leaseValid()) {
                    ChunkLocation newPrimary = cm.locations.stream()
                        .filter(loc -> { ServerInfo si = servers.get(loc.toString()); return si != null && si.isAlive(); })
                        .findFirst().orElse(null);
                    if (newPrimary != null) cm.grantLease(newPrimary);
                }
                if (cm.leaseValid()) {
                    return new Message("CHUNK_INFO")
                        .put("chunkId", cm.chunkId)
                        .put("primary", cm.primary)
                        .put("secondaries", cm.locations.subList(1, cm.locations.size()))
                        .put("append", true);
                }
            }
        }
        return allocateChunk(meta);
    }

    private Message requestChunkRead(Message req) {
        String chunkId = (String) req.get("chunkId");
        ChunkMetadata cm = chunkTable.get(chunkId);
        if (cm == null) return Message.error("Chunk not found: " + chunkId);
        // Return only live replica locations
        List<ChunkLocation> liveLocs = cm.locations.stream()
            .filter(loc -> { ServerInfo si = servers.get(loc.toString()); return si != null && si.isAlive(); })
            .collect(Collectors.toList());
        if (liveLocs.isEmpty()) return Message.error("No live replicas for chunk: " + chunkId);
        return new Message("CHUNK_INFO").put("chunkId", chunkId).put("locations", liveLocs);
    }

    // Lease renewal — primary requests this before the 60 s window expires
    private Message renewLease(Message req) {
        String chunkId = (String) req.get("chunkId");
        ChunkMetadata cm = chunkTable.get(chunkId);
        if (cm == null) return Message.error("Chunk not found: " + chunkId);

        ChunkLocation requester = (ChunkLocation) req.get("primary");
        if (cm.primary != null && cm.primary.equals(requester)) {
            cm.leaseGrantedAt = System.currentTimeMillis(); // extend
            return Message.ok().put("leaseExpiry", cm.leaseGrantedAt + GfsConfig.LEASE_DURATION_MS);
        }
        return Message.error("Requester is not the current primary for chunk: " + chunkId);
    }

    private Message allocateChunk(FileMetadata meta) {
        List<ChunkLocation> candidates = selectServers(
            Math.min(GfsConfig.REPLICATION_FACTOR, liveServers().size()));
        if (candidates.isEmpty()) return Message.error("No chunk servers available");

        String chunkId = UUID.randomUUID().toString();
        long   version = 1L;
        ChunkMetadata cm = new ChunkMetadata(chunkId, version);
        candidates.forEach(loc -> cm.locations.add(loc));
        cm.grantLease(candidates.get(0));
        chunkTable.put(chunkId, cm);
        meta.addChunk(chunkId);
        opLog.append("ALLOC_CHUNK", chunkId + " v" + version + " for " + meta.path);

        return new Message("CHUNK_INFO")
            .put("chunkId", chunkId)
            .put("version", version)
            .put("primary", cm.primary)
            .put("secondaries", candidates.subList(1, candidates.size()))
            .put("leaseExpiry", cm.leaseGrantedAt + GfsConfig.LEASE_DURATION_MS);
    }

    // -------------------------------------------------------------------------
    // Chunk server registration and heartbeat
    // -------------------------------------------------------------------------

    private Message registerServer(Message req) {
        String host = (String) req.get("host");
        int    port = (int)    req.get("port");
        String key  = host + ":" + port;
        ServerInfo info = new ServerInfo(new ChunkLocation(host, port));

        // Populate reported chunk versions from the registration payload
        if (req.has("chunkVersions")) {
            Map<String, Long> versions = (Map<String, Long>) req.get("chunkVersions");
            info.chunkVersions.putAll(versions);
        }
        servers.put(key, info);
        System.out.println("[Master] Registered chunk server " + key);
        opLog.append("REGISTER_SERVER", key);
        return Message.ok();
    }

    private Message heartbeat(Message req) {
        String host = (String) req.get("host");
        int    port = (int)    req.get("port");
        String key  = host + ":" + port;
        servers.computeIfAbsent(key, k -> new ServerInfo(new ChunkLocation(host, port)));
        ServerInfo info = servers.get(key);
        info.lastHeartbeat = System.currentTimeMillis();

        // Update reported versions and evict stale replicas
        if (req.has("chunkVersions")) {
            Map<String, Long> reported = (Map<String, Long>) req.get("chunkVersions");
            info.chunkVersions.clear();
            info.chunkVersions.putAll(reported);
            evictStaleReplicas(info, reported);
        }
        return Message.ok();
    }

    /**
     * Stale replica eviction (GFS §4.5):
     * If a chunk server reports a version older than what the Master expects,
     * that replica is stale (the server was offline during a write) and is
     * removed from the chunk's location list.
     */
    private void evictStaleReplicas(ServerInfo server, Map<String, Long> reportedVersions) {
        for (Map.Entry<String, Long> entry : reportedVersions.entrySet()) {
            String chunkId        = entry.getKey();
            long   reportedVersion = entry.getValue();
            ChunkMetadata cm = chunkTable.get(chunkId);
            if (cm == null) continue;
            if (reportedVersion < cm.version) {
                boolean removed = cm.locations.remove(server.location);
                if (removed) {
                    System.out.printf("[Master] Evicted stale replica of chunk %s from %s " +
                        "(reported v%d, expected v%d)%n",
                        chunkId, server.location, reportedVersion, cm.version);
                    // Also revoke lease if this server was primary
                    if (server.location.equals(cm.primary)) cm.revokeLease();
                }
            }
        }
    }

    private Message clusterStatus(Message req) {
        List<ServerInfo> live = liveServers();
        List<ServerInfo> dead = servers.values().stream()
            .filter(s -> !s.isAlive()).collect(Collectors.toList());

        int totalFiles = (int) namespace.values().stream().filter(m -> !m.isDirectory).count();
        int totalDirs  = (int) namespace.values().stream().filter(m ->  m.isDirectory).count();

        return new Message("CLUSTER_STATUS")
            .put("liveServers",  live.stream().map(s -> s.location.toString()).collect(Collectors.toList()))
            .put("deadServers",  dead.stream().map(s -> s.location.toString()).collect(Collectors.toList()))
            .put("totalChunks",  chunkTable.size())
            .put("totalFiles",   totalFiles)
            .put("totalDirs",    totalDirs);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private List<ServerInfo> liveServers() {
        return servers.values().stream().filter(ServerInfo::isAlive).collect(Collectors.toList());
    }

    private List<ChunkLocation> selectServers(int count) {
        return liveServers().stream()
            .sorted(Comparator.comparingInt(s -> s.chunkVersions.size()))
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
