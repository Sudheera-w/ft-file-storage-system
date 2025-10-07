package com.distributed.fs;

import com.distributed.fs.time.TimeSyncManager;
import com.distributed.fs.time.ClockedMessage;

public class MetadataNodeWithClock {
    private static final TimeSyncManager timeSync = new TimeSyncManager("metadata-1");

    public static void main(String[] args) {
        NodeManager nodeManager = new NodeManager();
        FileManager fileManager = new FileManager(nodeManager);

        nodeManager.registerNode("Node1", "localhost", 5001);
        nodeManager.registerNode("Node2", "localhost", 5002);
        nodeManager.registerNode("Node3", "localhost", 5003);

        new Thread(new HeartbeatService(nodeManager, fileManager)).start();
        new Thread(new FailureDetector(nodeManager)).start();
        new Thread(new RecoveryService(nodeManager, fileManager)).start();

        System.out.println("Metadata Node (with Lamport clock) started at time " + timeSync.getTime());
    }

    public static void onFileUpdate(ClockedMessage<String> msg) {
        timeSync.syncFromMessage(msg.getLamport());
        System.out.printf("[METADATA] Received '%s' from %s | Clock=%d%n",
                msg.getPayload(), msg.getFromNode(), timeSync.getTime());
    }
}
