package com.distributed.fs;

import java.io.*;
import java.nio.file.*;

public class Utils {

    /**
     * Writes content to a file with version metadata
     */
    public static void writeToFile(String path, String content, int version, long timestamp) throws IOException {
        File file = new File(path);
        file.getParentFile().mkdirs(); // Create parent directories if they don't exist

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.write("VERSION:" + version + "\n");
            writer.write("TIMESTAMP:" + timestamp + "\n");
            writer.write("CONTENT:\n");
            writer.write(content);
        }
    }

    /**
     * Reads a file and parses version metadata
     */
    public static FileVersion readFromFile(String path) throws IOException {
        File file = new File(path);
        if (!file.exists()) {
            return null;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String versionLine = reader.readLine();
            String timestampLine = reader.readLine();
            reader.readLine(); // Skip "CONTENT:" line

            int version = Integer.parseInt(versionLine.split(":")[1]);
            long timestamp = Long.parseLong(timestampLine.split(":")[1]);

            StringBuilder content = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }

            // Remove trailing newline if present
            String finalContent = content.toString();
            if (finalContent.endsWith("\n")) {
                finalContent = finalContent.substring(0, finalContent.length() - 1);
            }

            return new FileVersion(finalContent, version, timestamp);
        }
    }

    /**
     * Gets current timestamp in milliseconds
     */
    public static long getCurrentTimestamp() {
        return System.currentTimeMillis();
    }

    /**
     * Logs messages with timestamp
     */
    public static void log(String message) {
        System.out.println("[" + System.currentTimeMillis() + "] " + message);
    }

    /**
     * Checks if a file exists
     */
    public static boolean fileExists(String path) {
        return new File(path).exists();
    }

    /**
     * Deletes a file if it exists
     */
    public static void deleteFile(String path) throws IOException {
        File file = new File(path);
        if (file.exists()) {
            Files.delete(file.toPath());
        }
    }
}