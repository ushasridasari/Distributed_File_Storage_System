import java.io.*;
import java.util.List;

/**
 * GFS Java — Command-line entry point.
 *
 * Mirrors the reference DVCS project's Main.java which handles all 16 Git
 * commands from a single switch-case dispatch. All CLI commands live here;
 * the actual logic is delegated to GfsClient, MasterServer, and ChunkServer.
 *
 * Commands:
 *   gfs upload   <local-path> <gfs-path>     Upload a local file into GFS
 *   gfs download <gfs-path>   <local-path>   Download a GFS file locally
 *   gfs list     [prefix]                    List files under a path prefix
 *   gfs delete   <gfs-path>                  Delete a file from GFS
 *   gfs master   [port]                      Start the Master server
 *   gfs chunkserver <port> [storage-dir]     Start a Chunk server
 */
public class Main {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            printUsage();
            System.exit(1);
        }

        String masterHost = System.getProperty("gfs.master.host", GfsConfig.MASTER_HOST);
        int    masterPort = Integer.parseInt(
            System.getProperty("gfs.master.port", String.valueOf(GfsConfig.MASTER_PORT)));

        String command = args[0].toLowerCase();

        switch (command) {

            // ------------------------------------------------------------------
            // File operations (go through GfsClient -> Master -> ChunkServers)
            // ------------------------------------------------------------------

            case "upload": {
                requireArgs(args, 3, "upload <local-path> <gfs-path>");
                GfsClient client = new GfsClient(masterHost, masterPort);
                client.upload(args[1], args[2]);
                break;
            }

            case "download": {
                requireArgs(args, 3, "download <gfs-path> <local-path>");
                GfsClient client = new GfsClient(masterHost, masterPort);
                client.download(args[1], args[2]);
                break;
            }

            case "list": {
                String prefix = args.length > 1 ? args[1] : "/";
                GfsClient client = new GfsClient(masterHost, masterPort);
                List<String> files = client.listFiles(prefix);
                if (files == null || files.isEmpty()) {
                    System.out.println("(no files)");
                } else {
                    files.forEach(System.out::println);
                }
                break;
            }

            case "delete": {
                requireArgs(args, 2, "delete <gfs-path>");
                GfsClient client = new GfsClient(masterHost, masterPort);
                client.deleteFile(args[1]);
                break;
            }

            // ------------------------------------------------------------------
            // Server startup
            // ------------------------------------------------------------------

            case "master": {
                int port = args.length > 1 ? Integer.parseInt(args[1]) : GfsConfig.MASTER_PORT;
                GfsConfig.createDefaultConfig();
                MasterServer master = new MasterServer(port);
                Runtime.getRuntime().addShutdownHook(new Thread(master::stop));
                master.start();
                break;
            }

            case "chunkserver": {
                requireArgs(args, 2, "chunkserver <port> [storage-dir]");
                int    port       = Integer.parseInt(args[1]);
                String storageDir = args.length > 2 ? args[2] : "chunk_data/node_" + port;
                ChunkServer cs = new ChunkServer("localhost", port, storageDir);
                Runtime.getRuntime().addShutdownHook(new Thread(cs::stop));
                cs.start();
                break;
            }

            default:
                System.err.println("Unknown command: " + command);
                printUsage();
                System.exit(1);
        }
    }

    private static void requireArgs(String[] args, int min, String usage) {
        if (args.length < min) {
            System.err.println("Usage: gfs " + usage);
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println("GFS Java — Distributed File Storage System (GFS-inspired)");
        System.out.println();
        System.out.println("File operations:");
        System.out.println("  gfs upload      <local-path> <gfs-path>");
        System.out.println("  gfs download    <gfs-path>   <local-path>");
        System.out.println("  gfs list        [prefix]");
        System.out.println("  gfs delete      <gfs-path>");
        System.out.println();
        System.out.println("Server startup:");
        System.out.println("  gfs master      [port]                   (default: 9000)");
        System.out.println("  gfs chunkserver <port> [storage-dir]     (default port: 9100)");
        System.out.println();
        System.out.println("System properties:");
        System.out.println("  -Dgfs.master.host=<host>  (default: localhost)");
        System.out.println("  -Dgfs.master.port=<port>  (default: 9000)");
    }
}
