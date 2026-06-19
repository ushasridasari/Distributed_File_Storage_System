package com.gfs.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Namespace entry maintained by the Master. Maps a file path to its ordered list of chunk IDs.
 */
public class FileMetadata implements Serializable {
    private final String path;
    private final List<String> chunkIds; // ordered list
    private long createdAt;
    private long updatedAt;
    private long fileSize;

    public FileMetadata(String path) {
        this.path = path;
        this.chunkIds = new ArrayList<>();
        this.createdAt = System.currentTimeMillis();
        this.updatedAt = createdAt;
    }

    public String getPath() { return path; }
    public List<String> getChunkIds() { return chunkIds; }
    public long getFileSize() { return fileSize; }
    public void setFileSize(long fileSize) { this.fileSize = fileSize; }
    public long getCreatedAt() { return createdAt; }
    public long getUpdatedAt() { return updatedAt; }

    public void addChunk(String chunkId) {
        chunkIds.add(chunkId);
        updatedAt = System.currentTimeMillis();
    }
}
