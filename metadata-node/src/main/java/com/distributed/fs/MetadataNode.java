package com.distributed.fs;

public class MetadataNode {
    public static void main(String[] args) {
        NodeManager nodeManager = new NodeManager();
        FileManager fileManager = new FileManager(nodeManager);

        // Register nodes manually (or they register when starting storage nodes)
        nodeManager.registerNode("Node1", "localhost", 5001);
        nodeManager.registerNode("Node2", "localhost", 5002);
        nodeManager.registerNode("Node3", "localhost", 5003);

        // Start heartbeat listener
        new Thread(new HeartbeatService(nodeManager, fileManager)).start();

        // Start failure detector
        new Thread(new FailureDetector(nodeManager)).start();

        // Start recovery service
        new Thread(new RecoveryService(nodeManager, fileManager)).start();

        System.out.println("Metadata Node started.");
    }
}
