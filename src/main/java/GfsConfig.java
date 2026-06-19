import java.io.*;
import java.nio.file.*;

/**
 * Central configuration for the GFS cluster.
 * Mirrors how Config.java manages settings in the reference DVCS project.
 */
public class GfsConfig {

    public static final int    CHUNK_SIZE_BYTES         = 64 * 1024 * 1024; // 64 MB (GFS paper default)
    public static final int    REPLICATION_FACTOR       = 3;
    public static final String MASTER_HOST              = "localhost";
    public static final int    MASTER_PORT              = 9000;
    public static final int    CHUNK_SERVER_BASE_PORT   = 9100;
    public static final long   HEARTBEAT_INTERVAL_MS    = 5000;
    public static final long   CHUNK_SERVER_TIMEOUT_MS  = 15000;
    public static final long   CACHE_TTL_MS             = 60000;  // client-side chunk location cache TTL
    public static final long   LEASE_DURATION_MS        = 60000;  // chunk lease valid for 60 s (GFS paper §5.4)
    public static final long   CHECKPOINT_INTERVAL_MS   = 300000; // master checkpoint every 5 min

    // Network reliability
    public static final int    SOCKET_CONNECT_TIMEOUT_MS = 5000;  // max time to establish a TCP connection
    public static final int    SOCKET_READ_TIMEOUT_MS    = 30000; // max time to wait for a response
    public static final int    RPC_MAX_RETRIES           = 3;     // attempts before giving up
    public static final long   RPC_RETRY_DELAY_MS        = 500;   // base delay; multiplied by attempt number

    private static final String CONFIG_FILE = ".gfs/config";

    public static void createDefaultConfig() throws IOException {
        Path cfg = Paths.get(CONFIG_FILE);
        Files.createDirectories(cfg.getParent());
        if (Files.exists(cfg)) return;

        String defaults =
            "[core]\n" +
            "\tmasterHost       = " + MASTER_HOST            + "\n" +
            "\tmasterPort       = " + MASTER_PORT            + "\n" +
            "\tchunkSize        = " + CHUNK_SIZE_BYTES       + "\n" +
            "\treplication      = " + REPLICATION_FACTOR     + "\n" +
            "\tcacheTtlMs       = " + CACHE_TTL_MS           + "\n" +
            "\tleaseDurationMs  = " + LEASE_DURATION_MS      + "\n" +
            "\tcheckpointMs     = " + CHECKPOINT_INTERVAL_MS + "\n";
        Files.writeString(cfg, defaults);
    }

    public static String getConfigValue(String section, String key) {
        try {
            String current = null;
            for (String line : Files.readAllLines(Paths.get(CONFIG_FILE))) {
                line = line.trim();
                if (line.startsWith("[")) {
                    current = line.replaceAll("[\\[\\]]", "").trim();
                } else if (section.equals(current) && line.startsWith(key)) {
                    return line.split("=", 2)[1].trim();
                }
            }
        } catch (IOException ignored) {}
        return null;
    }
}
