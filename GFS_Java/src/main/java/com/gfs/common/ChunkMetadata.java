package com.gfs.common;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;

/**
 * Tracks which chunk servers hold replicas of a given chunk.
 */
public class ChunkMetadata implements Serializable {
    private final String chunkId;
    private final long version;
    private final List<ChunkLocation> locations;
    private ChunkLocation primary; // lease holder

    public ChunkMetadata(String chunkId, long version) {
        this.chunkId = chunkId;
        this.version = version;
        this.locations = new ArrayList<>();
    }

    public String getChunkId() { return chunkId; }
    public long getVersion() { return version; }
    public List<ChunkLocation> getLocations() { return locations; }
    public ChunkLocation getPrimary() { return primary; }
    public void setPrimary(ChunkLocation primary) { this.primary = primary; }
    public void addLocation(ChunkLocation loc) { locations.add(loc); }
}
