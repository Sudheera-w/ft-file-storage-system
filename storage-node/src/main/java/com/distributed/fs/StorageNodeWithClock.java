package com.distributed.fs;

import java.io.*;
import java.net.*;
import com.distributed.fs.time.*;

public class StorageNodeWithClock {
    private final String nodeId;
    private final FileStorage storage;
    private final int replicationPort;
    private final TimeSyncManager timeSync;

    public StorageNodeWithClock(String nodeId, String storagePath, int replicationPort) {
        this.nodeId = nodeId;
        this.storage = new FileStorage(nodeId, storagePath);
        this.replicationPort = replicationPort;
        this.timeSync = new TimeSyncManager(nodeId);
    }

    public void start(String metadataHost) {
        System.out.printf("Storage Node %s started (Lamport=%d)%n", nodeId, timeSync.getTime());
        new Thread(() -> startReplicationListener(metadataHost)).start();
    }

    private void startReplicationListener(String metadataHost) {
        try (ServerSocket serverSocket = new ServerSocket(replicationPort)) {
            while (true) {
                Socket client = serverSocket.accept();
                DataInputStream in = new DataInputStream(client.getInputStream());

                boolean fromClient = in.readBoolean();
                String fileName = in.readUTF();
                int len = in.readInt();
                byte[] data = new byte[len];
                in.readFully(data);
                long lamport = in.readLong();
                String fromNode = in.readUTF();

                timeSync.syncFromMessage(lamport);
                storage.storeFile(fileName, data);
                System.out.printf("[%s] Received %s from %s | Lamport=%d%n",
                        nodeId, fileName, fromNode, timeSync.getTime());

                notifyMetadataNode(fileName, metadataHost);
                client.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void notifyMetadataNode(String fileName, String metadataHost) {
        try (Socket socket = new Socket(metadataHost, Config.METADATA_NODE_PORT);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            long ts = timeSync.nextEvent();
            ClockedMessage<String> msg = new ClockedMessage<>("UPDATE:" + fileName, ts, nodeId);
            out.println(msg.getPayload() + ":" + msg.getLamport() + ":" + msg.getFromNode());
            System.out.printf("[%s] Notified metadata about %s | Lamport=%d%n", nodeId, fileName, ts);
        } catch (IOException e) {
            System.err.println("Notify failed: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        String nodeId = args.length > 0 ? args[0] : "Node1";
        String storagePath = args.length > 1 ? args[1] : "storage_" + nodeId;
        String metadataHost = args.length > 2 ? args[2] : "localhost";
        int port = args.length > 3 ? Integer.parseInt(args[3]) : getPort(nodeId);

        new StorageNodeWithClock(nodeId, storagePath, port).start(metadataHost);
    }

    private static int getPort(String id) {
        switch (id) {
            case "Node1": return 5001;
            case "Node2": return 5002;
            case "Node3": return 5003;
            default: return 9100;
        }
    }
}
