package com.gfs.master;

import com.gfs.common.ChunkMetadata;
import com.gfs.common.GfsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * GFS Master Server — single point of metadata coordination.
 *
 * Responsibilities (per GFS paper):
 *   - Maintain file namespace and chunk-to-location mapping
 *   - Grant chunk leases (primary election)
 *   - Direct chunk placement and replication
 *   - Process heartbeats from chunk servers
 */
public class MasterServer {
    private static final Logger log = LoggerFactory.getLogger(MasterServer.class);

    private final int port;
    private final Namespace namespace;
    private final ChunkServerRegistry registry;
    private final Map<String, ChunkMetadata> chunkTable;
    private final ExecutorService threadPool;
    private volatile boolean running;

    public MasterServer(int port) {
        this.port = port;
        this.namespace = new Namespace();
        this.registry = new ChunkServerRegistry();
        this.chunkTable = new ConcurrentHashMap<>();
        this.threadPool = Executors.newCachedThreadPool();
    }

    public void start() throws IOException {
        running = true;
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            log.info("MasterServer listening on port {}", port);
            while (running) {
                Socket client = serverSocket.accept();
                threadPool.submit(new MasterHandler(client, namespace, registry, chunkTable));
            }
        } finally {
            threadPool.shutdown();
        }
    }

    public void stop() {
        running = false;
        threadPool.shutdown();
    }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : GfsConfig.MASTER_PORT;
        MasterServer master = new MasterServer(port);
        Runtime.getRuntime().addShutdownHook(new Thread(master::stop));
        master.start();
    }
}
