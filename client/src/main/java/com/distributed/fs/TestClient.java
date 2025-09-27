package com.distributed.fs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.io.DataOutputStream;

public class TestClient {
    public static void main(String[] args) throws IOException {
        String fileName = "test.txt"; // file to upload
        byte[] data = new FileInputStream(new File(fileName)).readAllBytes();

        // Connect to Node1's replication listener port
        try (Socket socket = new Socket("localhost", Config.NODE1_PORT);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {

            // mark as client upload
            out.writeBoolean(true);
            out.writeUTF(fileName);
            out.writeInt(data.length);
            out.write(data);
        }

        System.out.println("File uploaded to Node1: " + fileName);
    }
}
