package com.gfs.client;

import com.gfs.common.ChunkLocation;
import com.gfs.common.GfsConfig;
import com.gfs.common.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * High-level GFS client API.
 *
 * Upload flow (mirrors GFS paper §3.1):
 *   1. Client asks Master to create file → gets back file handle
 *   2. For each 64 MB chunk: ask Master for chunk write lease
 *      → Master returns primary + secondaries
 *   3. Client writes chunk data to primary; primary chains replication to secondaries
 *
 * Download flow:
 *   1. Client asks Master for file info → gets ordered chunk ID list
 *   2. For each chunk ID: ask Master for chunk locations
 *   3. Read from any live replica (client picks)
 */
public class GfsClient {
    private static final Logger log = LoggerFactory.getLogger(GfsClient.class);

    private final MasterClient master;
    private final ChunkClient chunkClient;

    public GfsClient() {
        this(GfsConfig.MASTER_HOST, GfsConfig.MASTER_PORT);
    }

    public GfsClient(String masterHost, int masterPort) {
        this.master = new MasterClient(masterHost, masterPort);
        this.chunkClient = new ChunkClient();
    }

    /**
     * Upload a local file into GFS at the given path.
     */
    public void upload(String localPath, String gfsPath) throws IOException {
        File localFile = new File(localPath);
        if (!localFile.exists()) throw new FileNotFoundException("Local file not found: " + localPath);

        // 1. Create file entry in namespace
        Message createResp = master.createFile(gfsPath);
        if (createResp.getType() == Message.Type.ERROR) {
            throw new IOException("Cannot create file: " + createResp.get("error"));
        }
        log.info("Created GFS file: {}", gfsPath);

        // 2. Split into chunks and upload
        byte[] fileBytes = readAllBytes(localFile);
        int totalChunks = (int) Math.ceil((double) fileBytes.length / GfsConfig.CHUNK_SIZE_BYTES);
        totalChunks = Math.max(totalChunks, 1); // at least one chunk even for empty file

        for (int i = 0; i < totalChunks; i++) {
            int start = i * GfsConfig.CHUNK_SIZE_BYTES;
            int end = Math.min(start + GfsConfig.CHUNK_SIZE_BYTES, fileBytes.length);
            byte[] chunkData = Arrays.copyOfRange(fileBytes, start, end);

            Message chunkResp = master.requestChunkWrite(gfsPath);
            if (chunkResp.getType() == Message.Type.ERROR) {
                throw new IOException("Chunk allocation failed: " + chunkResp.get("error"));
            }

            String chunkId = chunkResp.get("chunkId");
            ChunkLocation primary = chunkResp.get("primary");
            List<ChunkLocation> secondaries = chunkResp.get("secondaries");
            if (secondaries == null) secondaries = new ArrayList<>();

            log.info("Uploading chunk {}/{} (chunkId={}) to primary={}", i + 1, totalChunks, chunkId, primary);
            chunkClient.writeChunk(primary, secondaries, chunkId, chunkData);
        }
        log.info("Upload complete: {} -> {} ({} chunk(s))", localPath, gfsPath, totalChunks);
    }

    /**
     * Download a GFS file and write it to a local path.
     */
    public void download(String gfsPath, String localPath) throws IOException {
        Message infoResp = master.getFileInfo(gfsPath);
        if (infoResp.getType() == Message.Type.ERROR) {
            throw new IOException("File not found: " + infoResp.get("error"));
        }

        List<String> chunkIds = infoResp.get("chunkIds");
        log.info("Downloading {} ({} chunks) -> {}", gfsPath, chunkIds.size(), localPath);

        try (FileOutputStream fos = new FileOutputStream(localPath)) {
            for (String chunkId : chunkIds) {
                Message locResp = master.requestChunkRead(chunkId);
                if (locResp.getType() == Message.Type.ERROR) {
                    throw new IOException("Chunk location error: " + locResp.get("error"));
                }
                List<ChunkLocation> locations = locResp.get("locations");
                if (locations == null || locations.isEmpty()) {
                    throw new IOException("No replicas available for chunk: " + chunkId);
                }
                // read from the first available replica
                byte[] data = chunkClient.readChunk(locations.get(0), chunkId);
                fos.write(data);
            }
        }
        log.info("Download complete: {}", localPath);
    }

    public List<String> listFiles(String prefix) throws IOException {
        Message resp = master.listFiles(prefix);
        return resp.get("files");
    }

    public void deleteFile(String gfsPath) throws IOException {
        Message resp = master.deleteFile(gfsPath);
        if (resp.getType() == Message.Type.ERROR) {
            throw new IOException("Delete failed: " + resp.get("error"));
        }
        log.info("Deleted GFS file: {}", gfsPath);
    }

    private byte[] readAllBytes(File file) throws IOException {
        try (FileInputStream fis = new FileInputStream(file)) {
            return fis.readAllBytes();
        }
    }
}
