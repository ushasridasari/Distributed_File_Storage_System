package com.gfs.chunkserver;

import com.gfs.common.ChunkLocation;
import com.gfs.common.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.List;

/**
 * Handles a single client or replication connection to the ChunkServer.
 */
public class ChunkServerHandler implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ChunkServerHandler.class);

    private final Socket socket;
    private final ChunkStorage storage;
    private final String serverHost;
    private final int serverPort;

    public ChunkServerHandler(Socket socket, ChunkStorage storage, String serverHost, int serverPort) {
        this.socket = socket;
        this.storage = storage;
        this.serverHost = serverHost;
        this.serverPort = serverPort;
    }

    @Override
    public void run() {
        try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {

            Message request = (Message) in.readObject();
            log.info("Received {} from {}", request.getType(), socket.getInetAddress());

            switch (request.getType()) {
                case WRITE_CHUNK:
                    handleWrite(request, out);
                    break;
                case READ_CHUNK:
                    handleRead(request, out);
                    break;
                case DELETE_CHUNK:
                    handleDelete(request, out);
                    break;
                case REPLICATE_CHUNK:
                    handleReplicate(request, out);
                    break;
                default:
                    out.writeObject(new Message(Message.Type.ERROR).put("error", "Unknown command"));
            }
        } catch (Exception e) {
            log.error("Error handling connection", e);
        } finally {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    private void handleWrite(Message req, ObjectOutputStream out) throws IOException {
        String chunkId = req.get("chunkId");
        byte[] data = req.get("data");
        storage.writeChunk(chunkId, data);

        // forward replication to secondaries if any
        List<ChunkLocation> secondaries = req.get("secondaries");
        if (secondaries != null) {
            for (ChunkLocation loc : secondaries) {
                replicateTo(chunkId, data, loc);
            }
        }

        out.writeObject(new Message(Message.Type.OK).put("chunkId", chunkId));
    }

    private void handleRead(Message req, ObjectOutputStream out) throws IOException {
        String chunkId = req.get("chunkId");
        byte[] data = storage.readChunk(chunkId);
        out.writeObject(new Message(Message.Type.OK).put("data", data));
    }

    private void handleDelete(Message req, ObjectOutputStream out) throws IOException {
        String chunkId = req.get("chunkId");
        storage.deleteChunk(chunkId);
        out.writeObject(new Message(Message.Type.OK));
    }

    private void handleReplicate(Message req, ObjectOutputStream out) throws IOException {
        String chunkId = req.get("chunkId");
        byte[] data = req.get("data");
        storage.writeChunk(chunkId, data);
        out.writeObject(new Message(Message.Type.OK).put("chunkId", chunkId));
    }

    private void replicateTo(String chunkId, byte[] data, ChunkLocation loc) {
        try (Socket s = new Socket(loc.getHost(), loc.getPort());
             ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(s.getInputStream())) {

            Message msg = new Message(Message.Type.REPLICATE_CHUNK)
                    .put("chunkId", chunkId)
                    .put("data", data);
            out.writeObject(msg);
            Message resp = (Message) in.readObject();
            log.info("Replicated chunk {} to {} -> {}", chunkId, loc, resp.getType());
        } catch (Exception e) {
            log.error("Replication to {} failed: {}", loc, e.getMessage());
        }
    }
}
