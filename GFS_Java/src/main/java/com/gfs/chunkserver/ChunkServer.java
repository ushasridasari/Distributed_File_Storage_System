package com.gfs.chunkserver;

import com.gfs.common.ChunkLocation;
import com.gfs.common.GfsConfig;
import com.gfs.common.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.util.Set;
import java.util.concurrent.*;

/**
 * GFS ChunkServer — stores chunk data and reports to Master.
 *
 * Responsibilities (per GFS paper):
 *   - Store chunks as flat files on local disk
 *   - Serve read/write requests from clients
 *   - Forward replication to secondaries
 *   - Send heartbeats to Master carrying chunk reports
 */
public class ChunkServer {
    private static final Logger log = LoggerFactory.getLogger(ChunkServer.class);

    private final String host;
    private final int port;
    private final ChunkStorage storage;
    private final ExecutorService threadPool;
    private final ScheduledExecutorService scheduler;
    private volatile boolean running;

    public ChunkServer(String host, int port, String storageDir) throws IOException {
        this.host = host;
        this.port = port;
        this.storage = new ChunkStorage(storageDir);
        this.threadPool = Executors.newCachedThreadPool();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    public void start() throws IOException {
        running = true;
        registerWithMaster();
        scheduleHeartbeats();

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            log.info("ChunkServer listening on {}:{}", host, port);
            while (running) {
                Socket client = serverSocket.accept();
                threadPool.submit(new ChunkServerHandler(client, storage, host, port));
            }
        }
    }

    private void registerWithMaster() {
        try (Socket s = new Socket(GfsConfig.MASTER_HOST, GfsConfig.MASTER_PORT);
             ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(s.getInputStream())) {

            Set<String> chunks = storage.listChunks();
            Message msg = new Message(Message.Type.REGISTER_CHUNK_SERVER)
                    .put("host", host)
                    .put("port", port)
                    .put("chunks", chunks);
            out.writeObject(msg);
            Message resp = (Message) in.readObject();
            log.info("Registered with Master: {}", resp.getType());
        } catch (Exception e) {
            log.warn("Could not register with master (will retry via heartbeat): {}", e.getMessage());
        }
    }

    private void scheduleHeartbeats() {
        scheduler.scheduleAtFixedRate(() -> {
            try (Socket s = new Socket(GfsConfig.MASTER_HOST, GfsConfig.MASTER_PORT);
                 ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                 ObjectInputStream in = new ObjectInputStream(s.getInputStream())) {

                Set<String> chunks = storage.listChunks();
                Message hb = new Message(Message.Type.HEARTBEAT)
                        .put("host", host)
                        .put("port", port)
                        .put("chunks", chunks);
                out.writeObject(hb);
                in.readObject(); // consume ACK
            } catch (Exception e) {
                log.warn("Heartbeat failed: {}", e.getMessage());
            }
        }, GfsConfig.HEARTBEAT_INTERVAL_MS, GfsConfig.HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        running = false;
        threadPool.shutdown();
        scheduler.shutdown();
    }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : GfsConfig.CHUNK_SERVER_BASE_PORT;
        String storageDir = args.length > 1 ? args[1] : "chunk_data/node_" + port;
        ChunkServer server = new ChunkServer("localhost", port, storageDir);
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        server.start();
    }
}
