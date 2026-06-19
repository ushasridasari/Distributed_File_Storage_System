package com.gfs.client;

import com.gfs.common.ChunkLocation;
import com.gfs.common.GfsConfig;
import com.gfs.common.Message;

import java.io.*;
import java.net.Socket;
import java.util.List;

/**
 * Thin RPC wrapper for talking to the MasterServer.
 */
public class MasterClient {
    private final String host;
    private final int port;

    public MasterClient() {
        this(GfsConfig.MASTER_HOST, GfsConfig.MASTER_PORT);
    }

    public MasterClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public Message createFile(String path) throws IOException {
        return send(new Message(Message.Type.CREATE_FILE).put("path", path));
    }

    public Message deleteFile(String path) throws IOException {
        return send(new Message(Message.Type.DELETE_FILE).put("path", path));
    }

    public Message listFiles(String prefix) throws IOException {
        return send(new Message(Message.Type.LIST_FILES).put("prefix", prefix));
    }

    public Message getFileInfo(String path) throws IOException {
        return send(new Message(Message.Type.GET_FILE_INFO).put("path", path));
    }

    public Message requestChunkWrite(String path) throws IOException {
        return send(new Message(Message.Type.REQUEST_CHUNK_WRITE).put("path", path));
    }

    public Message requestChunkRead(String chunkId) throws IOException {
        return send(new Message(Message.Type.REQUEST_CHUNK_READ).put("chunkId", chunkId));
    }

    private Message send(Message msg) throws IOException {
        try (Socket s = new Socket(host, port);
             ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(s.getInputStream())) {
            out.writeObject(msg);
            return (Message) in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Deserialization error", e);
        }
    }
}
