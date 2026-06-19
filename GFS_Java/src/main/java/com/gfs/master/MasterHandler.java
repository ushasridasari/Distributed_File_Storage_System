package com.gfs.master;

import com.gfs.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handles a single connection to the MasterServer.
 */
public class MasterHandler implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(MasterHandler.class);

    private final Socket socket;
    private final Namespace namespace;
    private final ChunkServerRegistry registry;
    private final Map<String, ChunkMetadata> chunkTable; // chunkId -> metadata

    public MasterHandler(Socket socket, Namespace namespace,
                         ChunkServerRegistry registry,
                         Map<String, ChunkMetadata> chunkTable) {
        this.socket = socket;
        this.namespace = namespace;
        this.registry = registry;
        this.chunkTable = chunkTable;
    }

    @Override
    public void run() {
        try (ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            Message req = (Message) in.readObject();
            log.debug("Master received: {}", req.getType());
            Message resp = dispatch(req);
            out.writeObject(resp);
        } catch (Exception e) {
            log.error("Handler error", e);
        } finally {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    private Message dispatch(Message req) {
        switch (req.getType()) {
            case CREATE_FILE:       return handleCreateFile(req);
            case DELETE_FILE:       return handleDeleteFile(req);
            case LIST_FILES:        return handleListFiles(req);
            case GET_FILE_INFO:     return handleGetFileInfo(req);
            case REQUEST_CHUNK_WRITE: return handleRequestChunkWrite(req);
            case REQUEST_CHUNK_READ:  return handleRequestChunkRead(req);
            case REGISTER_CHUNK_SERVER: return handleRegister(req);
            case HEARTBEAT:         return handleHeartbeat(req);
            default:
                return new Message(Message.Type.ERROR).put("error", "Unknown command: " + req.getType());
        }
    }

    private Message handleCreateFile(Message req) {
        String path = req.get("path");
        if (namespace.exists(path)) {
            return new Message(Message.Type.ERROR).put("error", "File exists: " + path);
        }
        namespace.create(path);
        return new Message(Message.Type.OK).put("path", path);
    }

    private Message handleDeleteFile(Message req) {
        String path = req.get("path");
        FileMetadata meta = namespace.delete(path);
        if (meta == null) {
            return new Message(Message.Type.ERROR).put("error", "File not found: " + path);
        }
        // chunk GC: remove metadata (actual deletion is lazy via heartbeat)
        for (String chunkId : meta.getChunkIds()) {
            chunkTable.remove(chunkId);
        }
        return new Message(Message.Type.OK);
    }

    private Message handleListFiles(Message req) {
        String prefix = req.has("prefix") ? (String) req.get("prefix") : "/";
        List<String> files = namespace.list(prefix);
        return new Message(Message.Type.FILE_LIST).put("files", files);
    }

    private Message handleGetFileInfo(Message req) {
        String path = req.get("path");
        FileMetadata meta = namespace.get(path);
        if (meta == null) {
            return new Message(Message.Type.ERROR).put("error", "File not found: " + path);
        }
        return new Message(Message.Type.FILE_INFO)
                .put("path", meta.getPath())
                .put("chunkIds", meta.getChunkIds())
                .put("fileSize", meta.getFileSize())
                .put("createdAt", meta.getCreatedAt());
    }

    private Message handleRequestChunkWrite(Message req) {
        String path = req.get("path");
        FileMetadata meta = namespace.get(path);
        if (meta == null) {
            return new Message(Message.Type.ERROR).put("error", "File not found: " + path);
        }

        String chunkId = UUID.randomUUID().toString();
        List<ChunkLocation> locations;
        try {
            locations = registry.selectForNewChunk(
                    Math.min(GfsConfig.REPLICATION_FACTOR, registry.getLiveServers().size()));
        } catch (IllegalStateException e) {
            return new Message(Message.Type.ERROR).put("error", e.getMessage());
        }

        ChunkMetadata chunkMeta = new ChunkMetadata(chunkId, 1L);
        locations.forEach(chunkMeta::addLocation);
        chunkMeta.setPrimary(locations.get(0));
        chunkTable.put(chunkId, chunkMeta);
        meta.addChunk(chunkId);

        return new Message(Message.Type.CHUNK_INFO)
                .put("chunkId", chunkId)
                .put("primary", chunkMeta.getPrimary())
                .put("secondaries", locations.subList(1, locations.size()));
    }

    private Message handleRequestChunkRead(Message req) {
        String chunkId = req.get("chunkId");
        ChunkMetadata chunkMeta = chunkTable.get(chunkId);
        if (chunkMeta == null) {
            return new Message(Message.Type.ERROR).put("error", "Chunk not found: " + chunkId);
        }
        return new Message(Message.Type.CHUNK_INFO)
                .put("chunkId", chunkId)
                .put("locations", chunkMeta.getLocations());
    }

    private Message handleRegister(Message req) {
        String host = req.get("host");
        int port = ((Number) req.get("port")).intValue();
        Set<String> chunks = req.get("chunks");
        registry.register(host, port, chunks != null ? chunks : new HashSet<>());
        return new Message(Message.Type.OK);
    }

    private Message handleHeartbeat(Message req) {
        String host = req.get("host");
        int port = ((Number) req.get("port")).intValue();
        Set<String> chunks = req.get("chunks");
        registry.heartbeat(host, port, chunks != null ? chunks : new HashSet<>());
        return new Message(Message.Type.OK);
    }
}
