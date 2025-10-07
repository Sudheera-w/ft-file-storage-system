package com.distributed.fs;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class StorageNodeNew {
    private final String nodeId;
    private final FileStorage storage;
    private final int replicationPort;

    public StorageNodeNew(String nodeId, String storagePath, int replicationPort) {
        this.nodeId = nodeId;
        this.storage = new FileStorage(nodeId, storagePath);
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

                    // Always notify metadata node about the file (whether from client or replication)
                    notifyMetadataNode(fileName, nodeId);

                    // Replicate only if uploaded from client
                    if (fromClient) {
                        replicateToOtherNodes(fileName, data);
                    }

                    client.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private void replicateToOtherNodes(String fileName, byte[] data) {
        System.out.println("Starting replication of " + fileName + " from " + nodeId);

        // Get list of other nodes to replicate to
        String[] otherNodes = getOtherNodes();

        for (String targetNode : otherNodes) {
            try {
                replicateToNode(targetNode, fileName, data);
            } catch (Exception e) {
                System.err.println("Failed to replicate " + fileName + " to " + targetNode + ": " + e.getMessage());
            }
        }
    }

    private void replicateToNode(String targetNode, String fileName, byte[] data) {
        int targetPort = getNodePort(targetNode);

        try (Socket socket = new Socket("localhost", targetPort);
             java.io.DataOutputStream out = new java.io.DataOutputStream(socket.getOutputStream())) {

            // Mark as replication (not from client)
            out.writeBoolean(false);
            out.writeUTF(fileName);
            out.writeInt(data.length);
            out.write(data);
            out.flush();

            System.out.println("Successfully replicated " + fileName + " to " + targetNode);

        } catch (IOException e) {
            System.err.println("Failed to replicate " + fileName + " to " + targetNode + ": " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private String[] getOtherNodes() {
        switch (nodeId) {
            case "Node1":
                return new String[]{"Node2", "Node3"};
            case "Node2":
                return new String[]{"Node1", "Node3"};
            case "Node3":
                return new String[]{"Node1", "Node2"};
            default:
                return new String[0];
        }
    }

    private int getNodePort(String nodeId) {
        switch (nodeId) {
            case "Node1": return 5001;
            case "Node2": return 5002;
            case "Node3": return 5003;
            default: return 9100;
        }
    }

    public static void main(String[] args) {
        String nodeId = args.length > 0 ? args[0] : "Node1";
        String storagePath = args.length > 1 ? args[1] : "storage_" + nodeId;
        String metadataHost = args.length > 2 ? args[2] : "localhost";
        int replicationPort = args.length > 3 ? Integer.parseInt(args[3]) : getDefaultPort(nodeId);

        StorageNodeNew node = new StorageNodeNew(nodeId, storagePath, replicationPort);
        node.start(metadataHost);
    }

    private void notifyMetadataNode(String fileName, String nodeId) {
        try (Socket socket = new Socket("localhost", Config.METADATA_NODE_PORT);
             java.io.PrintWriter out = new java.io.PrintWriter(socket.getOutputStream(), true)) {

            out.println(Message.FILE_UPDATE + ":" + fileName + ":" + nodeId);
            System.out.println("Notified metadata node about file: " + fileName);

        } catch (Exception e) {
            System.out.println("Failed to notify metadata node: " + e.getMessage());
        }
    }

    private static int getDefaultPort(String nodeId) {
        switch (nodeId) {
            case "Node1": return 5001;
            case "Node2": return 5002;
            case "Node3": return 5003;
            default: return 9100;
        }
    }
}