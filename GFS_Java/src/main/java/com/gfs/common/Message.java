package com.gfs.common;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Generic wire message exchanged between GFS components over TCP.
 */
public class Message implements Serializable {
    public enum Type {
        // Client -> Master
        CREATE_FILE, DELETE_FILE, LIST_FILES, GET_FILE_INFO,
        REQUEST_CHUNK_WRITE, REQUEST_CHUNK_READ,
        // ChunkServer -> Master
        REGISTER_CHUNK_SERVER, HEARTBEAT, REPORT_CHUNK,
        // Master -> Client/ChunkServer (responses)
        OK, ERROR, CHUNK_INFO, FILE_INFO, FILE_LIST,
        // Client -> ChunkServer
        WRITE_CHUNK, READ_CHUNK, DELETE_CHUNK,
        // ChunkServer -> ChunkServer (replication)
        REPLICATE_CHUNK
    }

    private final Type type;
    private final Map<String, Object> payload;

    public Message(Type type) {
        this.type = type;
        this.payload = new HashMap<>();
    }

    public Type getType() { return type; }

    public Message put(String key, Object value) {
        payload.put(key, value);
        return this;
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        return (T) payload.get(key);
    }

    public boolean has(String key) { return payload.containsKey(key); }

    @Override
    public String toString() { return "Message{type=" + type + ", payload=" + payload + "}"; }
}
