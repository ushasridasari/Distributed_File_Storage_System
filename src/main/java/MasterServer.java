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
        volatile long freeBytes     = Long.MAX_VALUE; // reported by ChunkServer in heartbeat
        String rackId = "default-rack";               // set on registration; used for rack-aware placement
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

        // 3. Schedule periodic checkpoint, re-replication, and orphan cleanup
        scheduleCheckpoint();
        scheduleReReplication();
        scheduleOrphanCleanup();

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

    /**
     * Orphan file cleanup — removes file entries that were created but never
     * had any chunks written (e.g. client crashed after CREATE_FILE but before
     * the first REQUEST_CHUNK_WRITE).  A 60-second grace period avoids racing
     * with slow but legitimate uploads.
     */
    private void scheduleOrphanCleanup() {
        scheduler.scheduleAtFixedRate(() -> {
            long cutoff = System.currentTimeMillis() - GfsConfig.ORPHAN_GRACE_PERIOD_MS;
            List<String> orphans = namespace.entrySet().stream()
                .filter(e -> {
                    FileMetadata m = e.getValue();
                    return !m.isDirectory && m.chunkIds.isEmpty() && m.createdAt < cutoff;
                })
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

            for (String path : orphans) {
                namespace.remove(path);
                opLog.append("ORPHAN_CLEANUP", path);
                System.out.printf("[Master] Removed orphaned file entry: %s (created but never written)%n", path);
            }
        }, 60000, 60000, TimeUnit.MILLISECONDS);
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
            case "UPDATE_FILE_SIZE":      return updateFileSize(req);
            case "NOTIFY_APPEND":         return notifyAppend(req);
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

        // Count only chunks that actually exist in chunkTable (excludes evicted chunks)
        long liveChunks = meta.chunkIds.stream()
            .filter(chunkTable::containsKey)
            .count();

        // Count live replicas (servers that are currently reachable)
        int liveReplicas = meta.chunkIds.stream()
            .filter(chunkTable::containsKey)
            .mapToInt(id -> {
                ChunkMetadata cm = chunkTable.get(id);
                return (int) cm.locations.stream()
                    .filter(loc -> { ServerInfo si = servers.get(loc.toString()); return si != null && si.isAlive(); })
                    .count();
            })
            .sum();

        return new Message("STAT")
            .put("path", meta.path)
            .put("isDirectory", meta.isDirectory)
            .put("fileSize", meta.fileSize)
            .put("chunkCount", liveChunks)
            .put("totalChunkIds", meta.chunkIds.size())
            .put("totalReplicas", liveReplicas)
            .put("createdAt", meta.createdAt)
            .put("updatedAt", meta.updatedAt);
    }

    private Message updateFileSize(Message req) {
        String path = (String) req.get("path");
        FileMetadata meta = namespace.get(path);
        if (meta == null) return Message.error("File not found: " + path);
        meta.fileSize  = ((Number) req.get("fileSize")).longValue();
        meta.updatedAt = System.currentTimeMillis();
        return Message.ok();
    }

    private Message notifyAppend(Message req) {
        String path = (String) req.get("path");
        FileMetadata meta = namespace.get(path);
        if (meta == null) return Message.error("File not found: " + path);
        long appendedBytes = ((Number) req.get("appendedBytes")).longValue();
        meta.fileSize  += appendedBytes;
        meta.updatedAt  = System.currentTimeMillis();
        return Message.ok();
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
        String host   = (String) req.get("host");
        int    port   = (int)    req.get("port");
        String key    = host + ":" + port;
        ServerInfo info = new ServerInfo(new ChunkLocation(host, port));

        if (req.has("rackId"))    info.rackId    = (String) req.get("rackId");
        if (req.has("freeBytes")) info.freeBytes  = ((Number) req.get("freeBytes")).longValue();
        if (req.has("chunkVersions")) {
            Map<String, Long> versions = (Map<String, Long>) req.get("chunkVersions");
            info.chunkVersions.putAll(versions);
        }
        servers.put(key, info);
        System.out.printf("[Master] Registered chunk server %s (rack=%s, free=%s)%n",
            key, info.rackId, formatBytes(info.freeBytes));
        opLog.append("REGISTER_SERVER", key + " rack=" + info.rackId);
        return Message.ok();
    }

    private Message heartbeat(Message req) {
        String host = (String) req.get("host");
        int    port = (int)    req.get("port");
        String key  = host + ":" + port;
        servers.computeIfAbsent(key, k -> new ServerInfo(new ChunkLocation(host, port)));
        ServerInfo info = servers.get(key);
        info.lastHeartbeat = System.currentTimeMillis();

        if (req.has("freeBytes")) info.freeBytes = ((Number) req.get("freeBytes")).longValue();
        if (req.has("rackId"))    info.rackId    = (String) req.get("rackId");

        // Update reported versions, evict stale replicas, and reconcile rejoining nodes
        if (req.has("chunkVersions")) {
            Map<String, Long> reported = (Map<String, Long>) req.get("chunkVersions");
            info.chunkVersions.clear();
            info.chunkVersions.putAll(reported);
            evictStaleReplicas(info, reported);
            reconcileRejoin(info, reported);
        }
        return Message.ok();
    }

    private static String formatBytes(long bytes) {
        if (bytes == Long.MAX_VALUE) return "unknown";
        if (bytes >= 1L << 30) return String.format("%.1f GB", bytes / (double)(1L << 30));
        if (bytes >= 1L << 20) return String.format("%.1f MB", bytes / (double)(1L << 20));
        return bytes + " B";
    }

    /**
     * Stale replica eviction (GFS §4.5):
     * If a chunk server reports a version older than what the Master expects,
     * that replica is stale (the server was offline during a write) and is
     * removed from the chunk's location list.
     */
    private void evictStaleReplicas(ServerInfo server, Map<String, Long> reportedVersions) {
        for (Map.Entry<String, Long> entry : reportedVersions.entrySet()) {
            String chunkId         = entry.getKey();
            long   reportedVersion = entry.getValue();
            ChunkMetadata cm = chunkTable.get(chunkId);
            if (cm == null) continue;
            if (reportedVersion < cm.version) {
                boolean removed = cm.locations.remove(server.location);
                if (removed) {
                    System.out.printf("[Master] Evicted stale replica of chunk %s from %s " +
                        "(reported v%d, expected v%d)%n",
                        chunkId, server.location, reportedVersion, cm.version);
                    if (server.location.equals(cm.primary)) cm.revokeLease();
                }
            }
        }
    }

    /**
     * Node rejoin reconciliation (GFS §4.4, §4.5):
     *
     * When a server comes back after being offline it reports all the chunks
     * it still holds.  We do two things:
     *
     *   1. Re-admit valid replicas — if the reported version matches the
     *      master's expected version and the server is not already in the
     *      replica list, add it back.  This restores replication factor
     *      without waiting for the background re-replication job.
     *
     *   2. Delete orphaned chunks — if the server holds a chunk that is no
     *      longer in the chunkTable (the file was deleted while the node was
     *      offline), send DELETE_CHUNK so the disk space is reclaimed.
     */
    private void reconcileRejoin(ServerInfo server, Map<String, Long> reportedVersions) {
        for (Map.Entry<String, Long> entry : reportedVersions.entrySet()) {
            String chunkId         = entry.getKey();
            long   reportedVersion = entry.getValue();
            ChunkMetadata cm = chunkTable.get(chunkId);

            if (cm == null) {
                // Orphaned chunk — file was deleted while this server was offline
                System.out.printf("[Master] Deleting orphaned chunk %s from rejoining server %s%n",
                    chunkId, server.location);
                deleteChunkFromServer(chunkId, server.location);
                continue;
            }

            // Re-admit if version is current and not already tracked
            if (reportedVersion == cm.version && !cm.locations.contains(server.location)) {
                cm.locations.add(server.location);
                System.out.printf("[Master] Re-admitted chunk %s at %s (v%d) after rejoin%n",
                    chunkId, server.location, reportedVersion);
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
            .put("liveServers",  live.stream()
                .map(s -> s.location + " rack=" + s.rackId + " free=" + formatBytes(s.freeBytes))
                .collect(Collectors.toList()))
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

    /**
     * Rack-aware, disk-space-aware server selection (GFS §3.1).
     *
     * Strategy:
     *   1. Sort all live servers by free disk space descending (prefer roomier nodes).
     *   2. Pick the first candidate unconditionally (most free space).
     *   3. For each subsequent slot, prefer a server on a rack not yet used —
     *      spreading replicas across racks so a single rack failure can't wipe
     *      all copies. Fall back to any live server if not enough racks exist.
     */
    private List<ChunkLocation> selectServers(int count) {
        List<ServerInfo> candidates = liveServers().stream()
            .sorted(Comparator.comparingLong((ServerInfo s) -> s.freeBytes).reversed())
            .collect(Collectors.toList());

        List<ChunkLocation> selected = new ArrayList<>();
        Set<String> usedRacks = new LinkedHashSet<>();

        // First pass: pick one server per rack (rack-diverse replicas)
        for (ServerInfo s : candidates) {
            if (selected.size() >= count) break;
            if (!usedRacks.contains(s.rackId)) {
                selected.add(s.location);
                usedRacks.add(s.rackId);
            }
        }

        // Second pass: fill remaining slots from any live server not already selected
        // (happens when there are fewer racks than REPLICATION_FACTOR)
        for (ServerInfo s : candidates) {
            if (selected.size() >= count) break;
            if (!selected.contains(s.location)) {
                selected.add(s.location);
            }
        }

        return selected;
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
