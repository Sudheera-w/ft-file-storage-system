package com.distributed.fs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

public class FileStorage {
    private final String storagePath;
    private final String nodeId;

    public FileStorage(String nodeId, String storagePath) {
        this.nodeId = nodeId;
        this.storagePath = storagePath;
        File dir = new File(storagePath);
        if (!dir.exists()) dir.mkdirs();
    }

    // Store file locally
    public void storeFile(String fileName, byte[] data, List<String> replicaNodes) throws IOException {
        saveToDisk(fileName, data);

        // Replicate to other nodes
        for (String replicaNodeId : replicaNodes) {
            if (!replicaNodeId.equals(this.nodeId)) {
                sendFileToNode(replicaNodeId, fileName, data);
            }
        }
    }

    private void saveToDisk(String fileName, byte[] data) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(storagePath + "/" + fileName)) {
            fos.write(data);
        }
    }

    // Simulated network send for replication (replace with actual RPC/network call)
    private void sendFileToNode(String nodeId, String fileName, byte[] data) {
        System.out.println("Replicating " + fileName + " from " + this.nodeId + " → " + nodeId);
        // TODO: Implement actual network transfer
    }

    public byte[] readFile(String fileName) throws IOException {
        File file = new File(storagePath + "/" + fileName);
        if (!file.exists()) return null;
        return Files.readAllBytes(file.toPath());
    }

    public boolean fileExists(String fileName) {
        File file = new File(storagePath + "/" + fileName);
        return file.exists();
    }
}
