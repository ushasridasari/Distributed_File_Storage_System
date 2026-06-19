package com.gfs.master;

import com.gfs.common.ChunkLocation;
import com.gfs.common.GfsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Tracks live chunk servers and their reported chunk sets.
 * Chunk server locations are rebuilt from heartbeats — not persisted.
 */
public class ChunkServerRegistry {
    private static final Logger log = LoggerFactory.getLogger(ChunkServerRegistry.class);

    private static class ServerInfo {
        final ChunkLocation location;
        volatile long lastHeartbeat;
        final Set<String> chunks;

        ServerInfo(ChunkLocation location) {
            this.location = location;
            this.lastHeartbeat = System.currentTimeMillis();
            this.chunks = ConcurrentHashMap.newKeySet();
        }
    }

    private final Map<String, ServerInfo> servers = new ConcurrentHashMap<>();

    private String key(String host, int port) { return host + ":" + port; }

    public void register(String host, int port, Set<String> chunks) {
        String k = key(host, port);
        ServerInfo info = new ServerInfo(new ChunkLocation(host, port));
        info.chunks.addAll(chunks);
        servers.put(k, info);
        log.info("Registered chunk server {}:{} with {} chunks", host, port, chunks.size());
    }

    public void heartbeat(String host, int port, Set<String> chunks) {
        String k = key(host, port);
        ServerInfo info = servers.computeIfAbsent(k, x -> new ServerInfo(new ChunkLocation(host, port)));
        info.lastHeartbeat = System.currentTimeMillis();
        info.chunks.clear();
        info.chunks.addAll(chunks);
    }

    public List<ChunkLocation> getLiveServers() {
        long now = System.currentTimeMillis();
        return servers.values().stream()
                .filter(s -> (now - s.lastHeartbeat) < GfsConfig.CHUNK_SERVER_TIMEOUT_MS)
                .map(s -> s.location)
                .collect(Collectors.toList());
    }

    /**
     * Pick N servers to host a new chunk, preferring least-loaded.
     */
    public List<ChunkLocation> selectForNewChunk(int count) {
        List<ServerInfo> live = servers.values().stream()
                .filter(s -> (System.currentTimeMillis() - s.lastHeartbeat) < GfsConfig.CHUNK_SERVER_TIMEOUT_MS)
                .sorted(Comparator.comparingInt(s -> s.chunks.size()))
                .collect(Collectors.toList());

        if (live.size() < count) {
            throw new IllegalStateException("Not enough chunk servers: need " + count + ", have " + live.size());
        }
        return live.subList(0, count).stream().map(s -> s.location).collect(Collectors.toList());
    }

    public int serverCount() { return servers.size(); }
}
