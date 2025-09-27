package com.distributed.fs;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class StorageNode {
    private final String nodeId;
    private final FileStorage storage;
    private final FileManager fileManager;
    private final NodeManager nodeManager;
    private final int replicationPort;

    public StorageNode(String nodeId, String storagePath, FileManager fileManager, NodeManager nodeManager, int replicationPort) {
        this.nodeId = nodeId;
        this.storage = new FileStorage(nodeId, storagePath);
        this.fileManager = fileManager;
        this.nodeManager = nodeManager;
        this.replicationPort = replicationPort;
    }

    public void start(String metadataHost) {
        System.out.println("Storage Node " + nodeId + " started.");

        // Heartbeat (optional)
        new Thread(new HeartbeatSender(metadataHost, Config.METADATA_NODE_PORT, nodeId)).start();

        // Start replication listener
        startReplicationListener();
    }

    private void startReplicationListener() {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(replicationPort)) {
                System.out.println("Node " + nodeId + " replication listener started on port " + replicationPort);
                while (true) {
                    Socket client = serverSocket.accept();
                    DataInputStream in = new DataInputStream(client.getInputStream());

                    boolean fromClient = in.readBoolean();
                    String fileName = in.readUTF();
                    int length = in.readInt();
                    byte[] data = new byte[length];
                    in.readFully(data);

                    storage.storeFile(fileName, data);
                    System.out.println("Received file: " + fileName + " on node " + nodeId);

                    // Replicate only if uploaded from client
                    if (fromClient) {
                        fileManager.ensureReplication(fileName, data, nodeId);
                    }

                    client.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    public static void main(String[] args) {
        String nodeId = args.length > 0 ? args[0] : "Node1";
        String storagePath = args.length > 1 ? args[1] : "storage_" + nodeId;
        String metadataHost = args.length > 2 ? args[2] : "localhost";
        int replicationPort = args.length > 3 ? Integer.parseInt(args[3]) : Config.STORAGE_NODE_PORT;

        NodeManager nodeManager = NodeManagerSingleton.getInstance();
        nodeManager.registerNode("Node1", "localhost", 5001);
        nodeManager.registerNode("Node2", "localhost", 5002);
        nodeManager.registerNode("Node3", "localhost", 5003);

        FileManager fileManager = new FileManager(nodeManager);

        StorageNode node = new StorageNode(nodeId, storagePath, fileManager, nodeManager, replicationPort);
        node.start(metadataHost);
    }
}
