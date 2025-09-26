package com.distributed.fs;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;

public class StorageNode {
    private final String nodeId;
    private final FileStorage storage;

    public StorageNode(String nodeId, String storagePath) {
        this.nodeId = nodeId;
        this.storage = new FileStorage(nodeId, storagePath);
    }

    public void start(String metadataHost) {
        System.out.println("Storage Node " + nodeId + " started.");

        // Start heartbeat to metadata node
        new Thread(new HeartbeatSender(metadataHost, Config.METADATA_NODE_PORT, nodeId)).start();

        // Start replication listener to receive replicated files
        startReplicationListener();
    }

    // Listener thread for receiving replicated files
    private void startReplicationListener() {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(Config.STORAGE_NODE_PORT)) {
                while (true) {
                    Socket client = serverSocket.accept();
                    DataInputStream in = new DataInputStream(client.getInputStream());

                    String fileName = in.readUTF();
                    int length = in.readInt();
                    byte[] data = new byte[length];
                    in.readFully(data);

                    // Save locally only; do not replicate further
                    storage.storeFile(fileName, data);
                    System.out.println("Received replicated file: " + fileName);

                    client.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    // Main method
    public static void main(String[] args) {
        String nodeId = args.length > 0 ? args[0] : "Node1";
        String storagePath = args.length > 1 ? args[1] : "storage_" + nodeId;
        String metadataHost = args.length > 2 ? args[2] : "localhost";

        StorageNode node = new StorageNode(nodeId, storagePath);
        node.start(metadataHost);
    }
}
