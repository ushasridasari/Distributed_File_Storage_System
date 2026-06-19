package com.gfs.cli;

import com.gfs.client.GfsClient;
import com.gfs.common.GfsConfig;

import java.util.List;

/**
 * Command-line interface for GFS operations.
 *
 * Usage:
 *   gfs upload   <local-path> <gfs-path>
 *   gfs download <gfs-path>   <local-path>
 *   gfs list     [prefix]
 *   gfs delete   <gfs-path>
 */
public class GfsCli {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            printUsage();
            System.exit(1);
        }

        String masterHost = System.getProperty("gfs.master.host", GfsConfig.MASTER_HOST);
        int masterPort = Integer.parseInt(System.getProperty("gfs.master.port",
                String.valueOf(GfsConfig.MASTER_PORT)));
        GfsClient client = new GfsClient(masterHost, masterPort);

        String command = args[0].toLowerCase();
        switch (command) {
            case "upload":
                requireArgs(args, 3, "upload <local-path> <gfs-path>");
                client.upload(args[1], args[2]);
                System.out.println("Uploaded: " + args[1] + " -> " + args[2]);
                break;

            case "download":
                requireArgs(args, 3, "download <gfs-path> <local-path>");
                client.download(args[1], args[2]);
                System.out.println("Downloaded: " + args[1] + " -> " + args[2]);
                break;

            case "list":
                String prefix = args.length > 1 ? args[1] : "/";
                List<String> files = client.listFiles(prefix);
                if (files.isEmpty()) {
                    System.out.println("(no files)");
                } else {
                    files.forEach(System.out::println);
                }
                break;

            case "delete":
                requireArgs(args, 2, "delete <gfs-path>");
                client.deleteFile(args[1]);
                System.out.println("Deleted: " + args[1]);
                break;

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
        System.out.println("GFS Java - Distributed File Storage System");
        System.out.println("Usage:");
        System.out.println("  gfs upload   <local-path> <gfs-path>");
        System.out.println("  gfs download <gfs-path>   <local-path>");
        System.out.println("  gfs list     [prefix]");
        System.out.println("  gfs delete   <gfs-path>");
        System.out.println();
        System.out.println("System properties:");
        System.out.println("  -Dgfs.master.host=<host>  (default: localhost)");
        System.out.println("  -Dgfs.master.port=<port>  (default: 9000)");
    }
}
