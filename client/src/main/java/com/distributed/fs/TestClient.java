package com.distributed.fs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.io.DataOutputStream;

public class TestClient {
    public static void main(String[] args) throws IOException {
        System.out.println("** testing **");

        // Upload test files to demonstrate recovery mechanism
        uploadFile("test.txt", "Hello from recovery test");
        uploadFile("sample.txt", "Replicating file");
        uploadFile("recovery.txt", "To test fault tolerance and recovery");

        System.out.println("\nAll test files uploaded successfully!");
    }

    private static void uploadFile(String fileName, String content) throws IOException {
        byte[] data = content.getBytes();

        // Connect to Node1's replication listener port
        try (Socket socket = new Socket("localhost", Config.NODE1_PORT);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {

            // mark as client upload
            out.writeBoolean(true);
            out.writeUTF(fileName);
            out.writeInt(data.length);
            out.write(data);
        }

        System.out.println("Uploaded: " + fileName + " -> " + content);
    }
}
