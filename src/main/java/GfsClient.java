import java.io.*;
import java.net.*;
import java.util.*;

/**
 * GFS Client — high-level API for uploading, downloading, listing, and deleting files.
 *
 * Mirrors how the reference DVCS project bundles all client-side workflow
 * (add -> commit -> checkout) in focused helper methods. All client-side
 * concerns live here:
 *   - Talk to Master for metadata (file info, chunk locations, lease grants)
 *   - Talk directly to ChunkServers for data (upload/download)
 *   - Split files into 64 MB chunks on upload; reassemble on download
 *
 * GFS paper upload flow (§3.1):
 *   1. Ask Master to create file entry
 *   2. For each chunk: ask Master for a write lease → get primary + secondaries
 *   3. Send chunk data to primary; primary chains replication to secondaries
 *
 * GFS paper download flow:
 *   1. Ask Master for file info → get ordered chunk ID list
 *   2. For each chunk ID: ask Master for replica locations
 *   3. Read from any live replica
 */
public class GfsClient {

    private final String masterHost;
    private final int    masterPort;

    public GfsClient() {
        this(GfsConfig.MASTER_HOST, GfsConfig.MASTER_PORT);
    }

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

        // 1. Create namespace entry on Master
        Message createResp = sendToMaster(new Message("CREATE_FILE").put("path", gfsPath));
        if ("ERROR".equals(createResp.type)) throw new IOException(createResp.get("error").toString());

        // 2. Split file into chunks and upload each one
        byte[] fileBytes  = readAllBytes(file);
        int totalChunks   = Math.max(1, (int) Math.ceil((double) fileBytes.length / GfsConfig.CHUNK_SIZE_BYTES));

        for (int i = 0; i < totalChunks; i++) {
            int    start     = i * GfsConfig.CHUNK_SIZE_BYTES;
            int    end       = Math.min(start + GfsConfig.CHUNK_SIZE_BYTES, fileBytes.length);
            byte[] chunkData = Arrays.copyOfRange(fileBytes, start, end);

            // Ask Master for a write lease (primary + secondaries)
            Message lease = sendToMaster(new Message("REQUEST_CHUNK_WRITE").put("path", gfsPath));
            if ("ERROR".equals(lease.type)) throw new IOException(lease.get("error").toString());

            String chunkId = (String) lease.get("chunkId");
            MasterServer.ChunkLocation primary = (MasterServer.ChunkLocation) lease.get("primary");
            List<MasterServer.ChunkLocation> secondaries =
                lease.has("secondaries") ? (List<MasterServer.ChunkLocation>) lease.get("secondaries") : new ArrayList<>();

            System.out.printf("[GfsClient] Uploading chunk %d/%d (id=%s) -> %s%n",
                i + 1, totalChunks, chunkId, primary);
            writeChunk(primary, secondaries, chunkId, chunkData);
        }
        System.out.printf("[GfsClient] Upload complete: %s -> %s (%d chunk(s))%n",
            localPath, gfsPath, totalChunks);
    }

    public void download(String gfsPath, String localPath) throws IOException {
        // 1. Get file info from Master
        Message info = sendToMaster(new Message("GET_FILE_INFO").put("path", gfsPath));
        if ("ERROR".equals(info.type)) throw new IOException(info.get("error").toString());

        List<String> chunkIds = (List<String>) info.get("chunkIds");
        System.out.printf("[GfsClient] Downloading %s (%d chunk(s)) -> %s%n",
            gfsPath, chunkIds.size(), localPath);

        // 2. Read each chunk from any available replica
        try (FileOutputStream fos = new FileOutputStream(localPath)) {
            for (String chunkId : chunkIds) {
                Message locResp = sendToMaster(new Message("REQUEST_CHUNK_READ").put("chunkId", chunkId));
                if ("ERROR".equals(locResp.type)) throw new IOException(locResp.get("error").toString());

                List<MasterServer.ChunkLocation> locations =
                    (List<MasterServer.ChunkLocation>) locResp.get("locations");
                if (locations == null || locations.isEmpty())
                    throw new IOException("No replicas for chunk: " + chunkId);

                byte[] data = readChunk(locations.get(0), chunkId);
                fos.write(data);
            }
        }
        System.out.println("[GfsClient] Download complete: " + localPath);
    }

    public List<String> listFiles(String prefix) throws IOException {
        Message resp = sendToMaster(new Message("LIST_FILES").put("prefix", prefix));
        return (List<String>) resp.get("files");
    }

    public void deleteFile(String gfsPath) throws IOException {
        Message resp = sendToMaster(new Message("DELETE_FILE").put("path", gfsPath));
        if ("ERROR".equals(resp.type)) throw new IOException(resp.get("error").toString());
        System.out.println("[GfsClient] Deleted: " + gfsPath);
    }

    // -------------------------------------------------------------------------
    // Chunk-level I/O
    // -------------------------------------------------------------------------

    private void writeChunk(MasterServer.ChunkLocation primary,
                            List<MasterServer.ChunkLocation> secondaries,
                            String chunkId, byte[] data) throws IOException {
        try (Socket s   = new Socket(primary.host, primary.port);
             ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
             ObjectInputStream  in  = new ObjectInputStream(s.getInputStream())) {

            out.writeObject(new Message("WRITE_CHUNK")
                .put("chunkId", chunkId)
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

    // -------------------------------------------------------------------------
    // Utility
    // -------------------------------------------------------------------------

    private byte[] readAllBytes(File file) throws IOException {
        try (FileInputStream fis = new FileInputStream(file)) {
            return fis.readAllBytes();
        }
    }
}
