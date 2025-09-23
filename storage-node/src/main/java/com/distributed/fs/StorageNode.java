package com.distributed.fs;

public class StorageNode {
    private final String nodeId;
    private final FileStorage storage;

    public StorageNode(String nodeId, String storagePath) {
        this.nodeId = nodeId;
        this.storage = new FileStorage(storagePath);
    }

    public void start(String metadataHost) {
        System.out.println("Storage Node " + nodeId + " started.");
        // Start heartbeat thread
        new Thread(new HeartbeatSender(metadataHost, Config.METADATA_NODE_PORT, nodeId)).start();
    }

    public static void main(String[] args) {
        String nodeId = args.length > 0 ? args[0] : "Node1";
        String storagePath = args.length > 1 ? args[1] : "storage_" + nodeId;
        String metadataHost = args.length > 2 ? args[2] : "localhost";

        StorageNode node = new StorageNode(nodeId, storagePath);
        node.start(metadataHost);
    }
}
