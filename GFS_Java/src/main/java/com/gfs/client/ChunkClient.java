package com.gfs.client;

import com.gfs.common.ChunkLocation;
import com.gfs.common.Message;

import java.io.*;
import java.net.Socket;
import java.util.List;

/**
 * Thin RPC wrapper for talking directly to a ChunkServer.
 */
public class ChunkClient {

    public void writeChunk(ChunkLocation primary, List<ChunkLocation> secondaries,
                           String chunkId, byte[] data) throws IOException {
        try (Socket s = new Socket(primary.getHost(), primary.getPort());
             ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(s.getInputStream())) {

            Message msg = new Message(Message.Type.WRITE_CHUNK)
                    .put("chunkId", chunkId)
                    .put("data", data)
                    .put("secondaries", secondaries);
            out.writeObject(msg);

            Message resp = (Message) in.readObject();
            if (resp.getType() == Message.Type.ERROR) {
                throw new IOException("ChunkServer write error: " + resp.get("error"));
            }
        } catch (ClassNotFoundException e) {
            throw new IOException("Deserialization error", e);
        }
    }

    public byte[] readChunk(ChunkLocation loc, String chunkId) throws IOException {
        try (Socket s = new Socket(loc.getHost(), loc.getPort());
             ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(s.getInputStream())) {

            out.writeObject(new Message(Message.Type.READ_CHUNK).put("chunkId", chunkId));
            Message resp = (Message) in.readObject();
            if (resp.getType() == Message.Type.ERROR) {
                throw new IOException("ChunkServer read error: " + resp.get("error"));
            }
            return resp.get("data");
        } catch (ClassNotFoundException e) {
            throw new IOException("Deserialization error", e);
        }
    }
}
