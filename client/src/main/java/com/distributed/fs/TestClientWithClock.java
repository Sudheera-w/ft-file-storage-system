package com.distributed.fs;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import com.distributed.fs.time.LamportClock;
import com.distributed.fs.time.ClockedMessage;

public class TestClientWithClock {

    // Client-side Lamport clock
    private static final LamportClock clock = new LamportClock("client-1");

    public static void main(String[] args) throws IOException {
        System.out.println("** Testing Client with Lamport Clock **");

        uploadFile("testnew.txt", "Hello from client with clock!");

        System.out.println("\nAll test files uploaded successfully with Lamport timestamps!");
    }

    private static void uploadFile(String fileName, String content) throws IOException {
        byte[] data = content.getBytes();

        // increment Lamport time for this send event
        long ts = clock.sendEvent();
        ClockedMessage<String> msg = new ClockedMessage<>(content, ts, clock.nodeId());

        try (Socket socket = new Socket("localhost", Config.NODE1_PORT);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {

            out.writeBoolean(true);
            out.writeUTF(fileName);
            out.writeInt(data.length);
            out.write(data);

            // include Lamport timestamp + sender ID
            out.writeLong(msg.getLamport());
            out.writeUTF(msg.getFromNode());

            System.out.printf("[CLIENT] Uploaded %s -> %s @ Lamport=%d%n",
                    fileName, msg.getPayload(), msg.getLamport());
        }
    }
}
