import java.io.*;
import java.nio.file.*;

/**
 * Central configuration for the GFS cluster.
 * Mirrors how Config.java manages settings in the reference DVCS project.
 */
public class GfsConfig {

    public static final int    CHUNK_SIZE_BYTES        = 64 * 1024 * 1024; // 64 MB (GFS paper default)
    public static final int    REPLICATION_FACTOR      = 3;
    public static final String MASTER_HOST             = "localhost";
    public static final int    MASTER_PORT             = 9000;
    public static final int    CHUNK_SERVER_BASE_PORT  = 9100;
    public static final long   HEARTBEAT_INTERVAL_MS   = 5000;
    public static final long   CHUNK_SERVER_TIMEOUT_MS = 15000;
    public static final long   CACHE_TTL_MS            = 60000; // client-side chunk location cache TTL

    private static final String CONFIG_FILE = ".gfs/config";

    public static void createDefaultConfig() throws IOException {
        Path cfg = Paths.get(CONFIG_FILE);
        Files.createDirectories(cfg.getParent());
        if (Files.exists(cfg)) return;

        String defaults =
            "[core]\n" +
            "\tmasterHost  = " + MASTER_HOST  + "\n" +
            "\tmasterPort  = " + MASTER_PORT  + "\n" +
            "\tchunkSize   = " + CHUNK_SIZE_BYTES + "\n" +
            "\treplication = " + REPLICATION_FACTOR + "\n" +
            "\tcacheTtlMs  = " + CACHE_TTL_MS + "\n";
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
