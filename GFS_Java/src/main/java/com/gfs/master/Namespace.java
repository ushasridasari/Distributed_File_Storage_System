package com.gfs.master;

import com.gfs.common.FileMetadata;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory file namespace. Maps absolute file paths to FileMetadata.
 * In production GFS this would be checkpointed to disk + operation log.
 */
public class Namespace {
    private final Map<String, FileMetadata> files = new ConcurrentHashMap<>();

    public FileMetadata create(String path) {
        if (files.containsKey(path)) {
            throw new IllegalStateException("File already exists: " + path);
        }
        FileMetadata meta = new FileMetadata(path);
        files.put(path, meta);
        return meta;
    }

    public FileMetadata get(String path) {
        return files.get(path);
    }

    public boolean exists(String path) {
        return files.containsKey(path);
    }

    public FileMetadata delete(String path) {
        return files.remove(path);
    }

    public List<String> list(String prefix) {
        List<String> result = new ArrayList<>();
        for (String p : files.keySet()) {
            if (p.startsWith(prefix)) result.add(p);
        }
        Collections.sort(result);
        return result;
    }
}
