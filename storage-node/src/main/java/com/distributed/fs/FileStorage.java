package com.distributed.fs;

import java.io.*;
import java.net.Socket;
import java.util.List;

public class FileStorage {
    private final String nodeId;
    private final String storagePath;

    public FileStorage(String nodeId, String storagePath) {
        this.nodeId = nodeId;
        this.storagePath = storagePath;

        File dir = new File(storagePath);
        if (!dir.exists()) dir.mkdirs();
    }

    // Store locally and optionally replicate
    public void storeFile(String fileName, byte[] data, List<String> replicateToNodes) throws IOException {
        // Store locally
        File file = new File(storagePath + "/" + fileName);
        try (FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(data);
        }

        // Replicate to other nodes if needed
        for (String targetNode : replicateToNodes) {
            if (!targetNode.equals(nodeId)) { // avoid sending to self
                sendFileToNode(targetNode, fileName, data);
            }
        }
    }

    // Overloaded method: store locally only
    public void storeFile(String fileName, byte[] data) throws IOException {
        storeFile(fileName, data, List.of());
    }

    // Send file to another storage node
    private void sendFileToNode(String targetHost, String fileName, byte[] data) {
        try (Socket socket = new Socket(targetHost, Config.STORAGE_NODE_PORT);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {

            out.writeUTF(fileName);
            out.writeInt(data.length);
            out.write(data);
            out.flush();

            System.out.println("Replicated " + fileName + " → " + targetHost);

        } catch (IOException e) {
            System.err.println("Failed to replicate " + fileName + " to " + targetHost + ": " + e.getMessage());
        }
    }

    public boolean fileExists(String fileName) {
        File file = new File(storagePath + "/" + fileName);
        return file.exists();
    }

    public byte[] readFile(String fileName) throws IOException {
        File file = new File(storagePath + "/" + fileName);
        if (!file.exists()) return null;
        return java.nio.file.Files.readAllBytes(file.toPath());
    }
}
