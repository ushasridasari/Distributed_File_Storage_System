import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * GFS ChunkServer — stores chunk data on local disk and serves clients.
 *
 * Mirrors how ObjectStore.java bundles all object read/write/compress logic
 * in the reference DVCS project. All chunk-server concerns live here:
 *   - Local disk storage (one flat file per chunk, named by UUID)
 *   - Read / write / delete request handling
 *   - Replication forwarding to secondary servers
 *   - Heartbeat sender to keep the Master updated
 *
 * GFS paper responsibilities:
 *   - Store chunks as flat files
 *   - Serve read/write from clients (primary chains writes to secondaries)
 *   - Send periodic heartbeats carrying the local chunk list
 */
public class ChunkServer {

    // -------------------------------------------------------------------------
    // Storage — flat files on local disk, one file per chunk
    // -------------------------------------------------------------------------

    static class ChunkStorage {
        private final Path dir;

        ChunkStorage(String storageDir) throws IOException {
            this.dir = Paths.get(storageDir);
            Files.createDirectories(this.dir);
        }

        void write(String chunkId, byte[] data) throws IOException {
            Files.write(dir.resolve(chunkId), data,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        }

        byte[] read(String chunkId) throws IOException {
            Path p = dir.resolve(chunkId);
            if (!Files.exists(p)) throw new FileNotFoundException("Chunk not found: " + chunkId);
            return Files.readAllBytes(p);
        }

        void delete(String chunkId) throws IOException {
            Files.deleteIfExists(dir.resolve(chunkId));
        }

        boolean has(String chunkId) {
            return Files.exists(dir.resolve(chunkId));
        }

        Set<String> list() throws IOException {
            Set<String> chunks = new HashSet<>();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
                for (Path entry : stream) chunks.add(entry.getFileName().toString());
            }
            return chunks;
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
                    case "WRITE_CHUNK":     handleWrite(req, out);     break;
                    case "READ_CHUNK":      handleRead(req, out);      break;
                    case "DELETE_CHUNK":    handleDelete(req, out);    break;
                    case "REPLICATE_CHUNK": handleReplicate(req, out); break;
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
            storage.write(chunkId, data);

            // chain replication to secondaries (GFS primary-push model)
            List<MasterServer.ChunkLocation> secondaries =
                req.has("secondaries") ? (List<MasterServer.ChunkLocation>) req.get("secondaries") : new ArrayList<>();
            for (MasterServer.ChunkLocation loc : secondaries) {
                replicateTo(chunkId, data, loc);
            }
            out.writeObject(Message.ok().put("chunkId", chunkId));
        }

        private void handleRead(Message req, ObjectOutputStream out) throws IOException {
            String chunkId = (String) req.get("chunkId");
            byte[] data = storage.read(chunkId);
            out.writeObject(Message.ok().put("data", data));
        }

        private void handleDelete(Message req, ObjectOutputStream out) throws IOException {
            storage.delete((String) req.get("chunkId"));
            out.writeObject(Message.ok());
        }

        private void handleReplicate(Message req, ObjectOutputStream out) throws IOException {
            storage.write((String) req.get("chunkId"), (byte[]) req.get("data"));
            out.writeObject(Message.ok());
        }

        private void replicateTo(String chunkId, byte[] data, MasterServer.ChunkLocation loc) {
            try (Socket s   = new Socket(loc.host, loc.port);
                 ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                 ObjectInputStream  in  = new ObjectInputStream(s.getInputStream())) {

                out.writeObject(new Message("REPLICATE_CHUNK")
                    .put("chunkId", chunkId)
                    .put("data", data));
                in.readObject(); // consume ACK
                System.out.println("[ChunkServer] Replicated " + chunkId + " -> " + loc);
            } catch (Exception e) {
                System.err.println("[ChunkServer] Replication to " + loc + " failed: " + e.getMessage());
            }
        }
    }

    // -------------------------------------------------------------------------
    // Server lifecycle
    // -------------------------------------------------------------------------

    private final String host;
    private final int port;
    private final ChunkStorage storage;
    private final ExecutorService pool      = Executors.newCachedThreadPool();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private volatile boolean running;

    public ChunkServer(String host, int port, String storageDir) throws IOException {
        this.host    = host;
        this.port    = port;
        this.storage = new ChunkStorage(storageDir);
    }

    public void start() throws IOException {
        running = true;
        registerWithMaster();
        scheduleHeartbeats();
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

    private Message buildHeartbeat(String type) {
        try {
            return new Message(type)
                .put("host", host)
                .put("port", port)
                .put("chunks", storage.list());
        } catch (IOException e) {
            return new Message(type).put("host", host).put("port", port);
        }
    }

    private void sendToMaster(Message msg) {
        try (Socket s   = new Socket(GfsConfig.MASTER_HOST, GfsConfig.MASTER_PORT);
             ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
             ObjectInputStream  in  = new ObjectInputStream(s.getInputStream())) {
            out.writeObject(msg);
            in.readObject(); // consume ACK
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
