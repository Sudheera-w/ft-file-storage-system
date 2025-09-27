package com.distributed.fs;

import java.io.*;

public class FileStorage {
    private final String nodeId;
    private final String storagePath;

    public FileStorage(String nodeId, String storagePath) {
        this.nodeId = nodeId;
        this.storagePath = storagePath;

        File dir = new File(storagePath);
        if (!dir.exists()) dir.mkdirs();
    }

    // Store file locally
    public void storeFile(String fileName, byte[] data) throws IOException {
        File file = new File(storagePath + "/" + fileName);
        try (FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(data);
        }
        System.out.println("Stored file " + fileName + " locally on " + nodeId);
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
