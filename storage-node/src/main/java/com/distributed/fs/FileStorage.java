package com.distributed.fs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;

public class FileStorage {
    private final String storagePath;

    public FileStorage(String storagePath) {
        this.storagePath = storagePath;
        File dir = new File(storagePath);
        if (!dir.exists()) dir.mkdirs();
    }

    public void storeFile(String fileName, byte[] data) throws IOException {
        FileOutputStream fos = new FileOutputStream(storagePath + "/" + fileName);
        fos.write(data);
        fos.close();
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
