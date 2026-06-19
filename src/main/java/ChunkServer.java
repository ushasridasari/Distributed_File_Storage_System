import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.zip.CRC32;

/**
 * GFS ChunkServer — stores chunk data on local disk and serves clients.
 *
 * Mirrors how ObjectStore.java bundles all object read/write/compress logic
 * in the reference DVCS project. All chunk-server concerns live here:
 *   - Local disk storage (one flat file per chunk, named by UUID)
 *   - Version sidecar (.ver) per chunk for stale replica detection
 *   - CRC32 checksum sidecar (.crc) per chunk for integrity verification
 *   - Read / write / append / delete / verify request handling
 *   - Replication forwarding to secondary servers
 *   - Heartbeat with chunk version map sent to Master
 *   - Lease renewal requests before the 60 s window expires
 *
 * GFS paper responsibilities (§2.6, §3, §4):
 *   - Store chunks as flat files
 *   - Report chunk versions in heartbeats so Master can evict stale replicas
 *   - Serve read/write/append from clients (primary chains to secondaries)
 *   - Detect and report data corruption via checksums
 */
public class ChunkServer {

    // -------------------------------------------------------------------------
    // Storage — flat files; .crc and .ver sidecars alongside each chunk
    // -------------------------------------------------------------------------

    static class ChunkStorage {
        private final Path dir;

        ChunkStorage(String storageDir) throws IOException {
            this.dir = Paths.get(storageDir);
            Files.createDirectories(this.dir);
        }

        void write(String chunkId, byte[] data, long version) throws IOException {
            Files.write(dir.resolve(chunkId), data,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            writeCrc(chunkId, computeCrc(data));
            writeVersion(chunkId, version);
        }

        void append(String chunkId, byte[] data, long version) throws IOException {
            Path p = dir.resolve(chunkId);
            byte[] existing = Files.exists(p) ? Files.readAllBytes(p) : new byte[0];
            byte[] combined = new byte[existing.length + data.length];
            System.arraycopy(existing, 0, combined, 0, existing.length);
            System.arraycopy(data, 0, combined, existing.length, data.length);
            Files.write(p, combined, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            writeCrc(chunkId, computeCrc(combined));
            writeVersion(chunkId, version);
        }

        byte[] read(String chunkId) throws IOException {
            Path p = dir.resolve(chunkId);
            if (!Files.exists(p)) throw new FileNotFoundException("Chunk not found: " + chunkId);
            byte[] data = Files.readAllBytes(p);
            verifyCrc(chunkId, data);
            return data;
        }

        void delete(String chunkId) throws IOException {
            Files.deleteIfExists(dir.resolve(chunkId));
            Files.deleteIfExists(dir.resolve(chunkId + ".crc"));
            Files.deleteIfExists(dir.resolve(chunkId + ".ver"));
        }

        boolean has(String chunkId) {
            return Files.exists(dir.resolve(chunkId));
        }

        long size(String chunkId) throws IOException {
            Path p = dir.resolve(chunkId);
            return Files.exists(p) ? Files.size(p) : 0;
        }

        long readVersion(String chunkId) {
            try {
                Path p = dir.resolve(chunkId + ".ver");
                return Files.exists(p) ? Long.parseLong(Files.readString(p).trim()) : 1L;
            } catch (IOException e) { return 1L; }
        }

        // Returns chunkId -> version map for all stored chunks (sent in heartbeat)
        Map<String, Long> listWithVersions() throws IOException {
            Map<String, Long> result = new HashMap<>();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
                for (Path entry : stream) {
                    String name = entry.getFileName().toString();
                    if (!name.endsWith(".crc") && !name.endsWith(".ver"))
                        result.put(name, readVersion(name));
                }
            }
            return result;
        }

        Set<String> list() throws IOException {
            return listWithVersions().keySet();
        }

        // ---- sidecar helpers ----

        private long computeCrc(byte[] data) {
            CRC32 crc = new CRC32();
            crc.update(data);
            return crc.getValue();
        }

        private void writeCrc(String chunkId, long crc) throws IOException {
            Files.writeString(dir.resolve(chunkId + ".crc"), String.valueOf(crc),
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        }

        private void verifyCrc(String chunkId, byte[] data) throws IOException {
            Path crcFile = dir.resolve(chunkId + ".crc");
            if (!Files.exists(crcFile)) return;
            long stored   = Long.parseLong(Files.readString(crcFile).trim());
            long computed = computeCrc(data);
            if (stored != computed)
                throw new IOException("Checksum mismatch for chunk " + chunkId +
                    " (stored=" + stored + ", computed=" + computed + ")");
        }

        private void writeVersion(String chunkId, long version) throws IOException {
            Files.writeString(dir.resolve(chunkId + ".ver"), String.valueOf(version),
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        }
    }

    // -------------------------------------------------------------------------
    // Handler — one thread per connection
    // -------------------------------------------------------------------------

    static class Handler implements Runnable {
        private final Socket socket;
        private final ChunkStorage storage;

        Handler(Socket socket, ChunkStorage storage) {
            this.socket  = socket;
            this.storage = storage;
        }

        @Override
        public void run() {
            try (ObjectInputStream  in  = new ObjectInputStream(socket.getInputStream());
                 ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {

                Message req = (Message) in.readObject();
                switch (req.type) {
                    case "WRITE_CHUNK":     handleWrite(req, out);    break;
                    case "APPEND_CHUNK":    handleAppend(req, out);   break;
                    case "READ_CHUNK":      handleRead(req, out);     break;
                    case "DELETE_CHUNK":    handleDelete(req, out);   break;
                    case "VERIFY_CHUNK":    handleVerify(req, out);   break;
                    case "REPLICATE_CHUNK":
                    case "REPLICATE_APPEND":handleReplicate(req, out); break;
                    default:
                        out.writeObject(Message.error("Unknown: " + req.type));
                }
            } catch (Exception e) {
                System.err.println("[ChunkServer] Handler error: " + e.getMessage());
            } finally {
                try { socket.close(); } catch (IOException ignored) {}
            }
        }

        private void handleWrite(Message req, ObjectOutputStream out) throws IOException {
            String chunkId = (String) req.get("chunkId");
            byte[] data    = (byte[]) req.get("data");
            long   version = req.has("version") ? ((Number) req.get("version")).longValue() : 1L;
            storage.write(chunkId, data, version);

            List<MasterServer.ChunkLocation> secondaries = getSecondaries(req);
            for (MasterServer.ChunkLocation loc : secondaries)
                replicateTo("REPLICATE_CHUNK", chunkId, data, version, loc);

            out.writeObject(Message.ok().put("chunkId", chunkId));
        }

        private void handleAppend(Message req, ObjectOutputStream out) throws IOException {
            String chunkId = (String) req.get("chunkId");
            byte[] data    = (byte[]) req.get("data");
            long   version = req.has("version") ? ((Number) req.get("version")).longValue() : 1L;
            long   offset  = storage.size(chunkId);
            storage.append(chunkId, data, version);

            List<MasterServer.ChunkLocation> secondaries = getSecondaries(req);
            for (MasterServer.ChunkLocation loc : secondaries)
                replicateTo("REPLICATE_APPEND", chunkId, data, version, loc);

            out.writeObject(Message.ok().put("chunkId", chunkId).put("offset", offset));
        }

        private void handleRead(Message req, ObjectOutputStream out) throws IOException {
            String chunkId = (String) req.get("chunkId");
            byte[] data = storage.read(chunkId); // CRC verified inside
            out.writeObject(Message.ok().put("data", data));
        }

        private void handleDelete(Message req, ObjectOutputStream out) throws IOException {
            storage.delete((String) req.get("chunkId"));
            out.writeObject(Message.ok());
        }

        private void handleVerify(Message req, ObjectOutputStream out) throws IOException {
            String chunkId = (String) req.get("chunkId");
            try {
                storage.read(chunkId);
                out.writeObject(Message.ok().put("chunkId", chunkId).put("valid", true));
            } catch (IOException e) {
                out.writeObject(Message.ok().put("chunkId", chunkId).put("valid", false)
                    .put("error", e.getMessage()));
            }
        }

        private void handleReplicate(Message req, ObjectOutputStream out) throws IOException {
            String chunkId = (String) req.get("chunkId");
            byte[] data    = (byte[]) req.get("data");
            long   version = req.has("version") ? ((Number) req.get("version")).longValue() : 1L;
            if ("REPLICATE_APPEND".equals(req.type)) {
                storage.append(chunkId, data, version);
            } else {
                storage.write(chunkId, data, version);
            }
            out.writeObject(Message.ok());
        }

        private void replicateTo(String msgType, String chunkId, byte[] data,
                                 long version, MasterServer.ChunkLocation loc) {
            try (Socket s   = new Socket(loc.host, loc.port);
                 ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                 ObjectInputStream  in  = new ObjectInputStream(s.getInputStream())) {

                out.writeObject(new Message(msgType)
                    .put("chunkId", chunkId)
                    .put("data", data)
                    .put("version", version));
                in.readObject();
                System.out.println("[ChunkServer] Replicated " + chunkId + " -> " + loc);
            } catch (Exception e) {
                System.err.println("[ChunkServer] Replication to " + loc + " failed: " + e.getMessage());
            }
        }

        @SuppressWarnings("unchecked")
        private List<MasterServer.ChunkLocation> getSecondaries(Message req) {
            return req.has("secondaries")
                ? (List<MasterServer.ChunkLocation>) req.get("secondaries")
                : new ArrayList<>();
        }
    }

    // -------------------------------------------------------------------------
    // Server lifecycle
    // -------------------------------------------------------------------------

    private final String host;
    private final int port;
    private final ChunkStorage storage;
    private final ExecutorService pool = Executors.newCachedThreadPool();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private volatile boolean running;

    // Tracks chunks this server is primary for, with lease expiry time
    private final Map<String, Long> primaryLeases = new ConcurrentHashMap<>();

    public ChunkServer(String host, int port, String storageDir) throws IOException {
        this.host    = host;
        this.port    = port;
        this.storage = new ChunkStorage(storageDir);
    }

    public void start() throws IOException {
        running = true;
        registerWithMaster();
        scheduleHeartbeats();
        scheduleLeaseRenewals();
        System.out.println("[ChunkServer] Listening on " + host + ":" + port);
        try (ServerSocket ss = new ServerSocket(port)) {
            while (running) {
                Socket client = ss.accept();
                pool.submit(new Handler(client, storage));
            }
        }
    }

    public void stop() { running = false; pool.shutdown(); scheduler.shutdown(); }

    private void registerWithMaster() {
        sendToMaster(buildHeartbeat("REGISTER_CHUNK_SERVER"));
    }

    private void scheduleHeartbeats() {
        scheduler.scheduleAtFixedRate(
            () -> sendToMaster(buildHeartbeat("HEARTBEAT")),
            GfsConfig.HEARTBEAT_INTERVAL_MS,
            GfsConfig.HEARTBEAT_INTERVAL_MS,
            TimeUnit.MILLISECONDS);
    }

    // Lease renewal: if we hold a primary lease expiring within 10 s, renew it
    private void scheduleLeaseRenewals() {
        scheduler.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            for (Map.Entry<String, Long> entry : primaryLeases.entrySet()) {
                long expiresAt = entry.getValue();
                if (expiresAt - now < 10_000) { // renew 10 s before expiry
                    renewLease(entry.getKey());
                }
            }
        }, 5000, 5000, TimeUnit.MILLISECONDS);
    }

    private void renewLease(String chunkId) {
        try (Socket s   = new Socket(GfsConfig.MASTER_HOST, GfsConfig.MASTER_PORT);
             ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
             ObjectInputStream  in  = new ObjectInputStream(s.getInputStream())) {

            out.writeObject(new Message("RENEW_LEASE")
                .put("chunkId", chunkId)
                .put("primary", new MasterServer.ChunkLocation(host, port)));
            Message resp = (Message) in.readObject();
            if ("OK".equals(resp.type)) {
                long newExpiry = ((Number) resp.get("leaseExpiry")).longValue();
                primaryLeases.put(chunkId, newExpiry);
            } else {
                primaryLeases.remove(chunkId); // revoked by master
            }
        } catch (Exception e) {
            System.err.println("[ChunkServer] Lease renewal failed for " + chunkId + ": " + e.getMessage());
        }
    }

    private Message buildHeartbeat(String type) {
        try {
            Map<String, Long> versions = storage.listWithVersions();
            return new Message(type)
                .put("host", host)
                .put("port", port)
                .put("chunkVersions", versions); // versions map sent instead of plain chunk set
        } catch (IOException e) {
            return new Message(type).put("host", host).put("port", port);
        }
    }

    private void sendToMaster(Message msg) {
        try (Socket s   = new Socket(GfsConfig.MASTER_HOST, GfsConfig.MASTER_PORT);
             ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
             ObjectInputStream  in  = new ObjectInputStream(s.getInputStream())) {
            out.writeObject(msg);
            in.readObject();
        } catch (Exception e) {
            System.err.println("[ChunkServer] Master contact failed: " + e.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // Entry point
    // -------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : GfsConfig.CHUNK_SERVER_BASE_PORT;
        String dir = args.length > 1 ? args[1] : "chunk_data/node_" + port;
        ChunkServer server = new ChunkServer("localhost", port, dir);
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        server.start();
    }
}
