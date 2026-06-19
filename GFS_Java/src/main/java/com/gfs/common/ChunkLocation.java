package com.gfs.common;

import java.io.Serializable;
import java.util.Objects;

public class ChunkLocation implements Serializable {
    private final String host;
    private final int port;

    public ChunkLocation(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() { return host; }
    public int getPort() { return port; }

    @Override
    public String toString() { return host + ":" + port; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ChunkLocation)) return false;
        ChunkLocation that = (ChunkLocation) o;
        return port == that.port && Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() { return Objects.hash(host, port); }
}
