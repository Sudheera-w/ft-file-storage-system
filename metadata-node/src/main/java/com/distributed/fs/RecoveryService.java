package com.distributed.fs;

import java.util.Map;
import java.util.Set;

public class RecoveryService implements Runnable {
    private final NodeManager nodeManager;
    private final FileManager fileManager;

    public RecoveryService(NodeManager nodeManager, FileManager fileManager) {
        this.nodeManager = nodeManager;
        this.fileManager = fileManager;
    }

    @Override
    public void run() {
        while (true) {
            try {
                for (Map.Entry<String, NodeInfo> entry : nodeManager.getNodes().entrySet()) {
                    NodeInfo node = entry.getValue();
                    if (node.isOnline()) continue; // Only handle failed nodes

                    System.out.println("Detected failed node: " + node.getNodeId());

                    // For all files in metadata, check if they were on the failed node
                    for (String fileName : fileManager.getAllFiles()) {
                        Set<String> nodesWithFile = fileManager.getNodesForFile(fileName);
                        if (!nodesWithFile.contains(node.getNodeId())) continue;

                        // Choose a healthy source node
                        String sourceNodeId = nodesWithFile.stream()
                                .filter(id -> !id.equals(node.getNodeId()) && nodeManager.getNodes().get(id).isOnline())
                                .findFirst()
                                .orElse(null);

                        if (sourceNodeId == null) {
                            System.out.println("No healthy source available for file: " + fileName);
                            continue;
                        }

                        NodeInfo sourceNode = nodeManager.getNodes().get(sourceNodeId);

                        // Fetch file data from source node
                        byte[] fileData = fileManager.getFileDataFromNode(sourceNode, fileName);
                        if (fileData == null) continue;

                        // Replicate to recovered node
                        try {
                            fileManager.replicateFileToTargetNode(fileName, fileData, node);
                            System.out.println("Re-replicated file " + fileName + " from "
                                    + sourceNodeId + " -> " + node.getNodeId());
                        } catch (Exception e) {
                            System.out.println("Failed to replicate " + fileName + " to " + node.getNodeId());
                            e.printStackTrace();
                        }
                    }
                }

                Thread.sleep(5000); // check every 5 seconds
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
