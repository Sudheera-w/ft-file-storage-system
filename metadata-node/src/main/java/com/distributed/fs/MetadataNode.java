package com.distributed.fs;

public class MetadataNode {
    public static void main(String[] args) {
        NodeManager nodeManager = new NodeManager();
        FileManager fileManager = new FileManager(nodeManager);

        // Start heartbeat listener
        new Thread(new HeartbeatService(nodeManager)).start();

        // Start recovery service
        new Thread(new RecoveryService(nodeManager, fileManager)).start();

        System.out.println("Metadata Node started.");
    }
}
