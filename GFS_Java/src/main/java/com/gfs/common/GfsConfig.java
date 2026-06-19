package com.gfs.common;

public class GfsConfig {
    public static final int CHUNK_SIZE_BYTES = 64 * 1024 * 1024; // 64 MB like GFS
    public static final int REPLICATION_FACTOR = 3;
    public static final int MASTER_PORT = 9000;
    public static final String MASTER_HOST = "localhost";
    public static final int CHUNK_SERVER_BASE_PORT = 9100;
    public static final long HEARTBEAT_INTERVAL_MS = 5000;
    public static final long CHUNK_SERVER_TIMEOUT_MS = 15000;
}
