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
                // Handle recovering nodes (nodes that just came back online)
                handleRecoveringNodes();

                // Handle failed nodes (nodes that are offline)
                handleFailedNodes();

                Thread.sleep(5000); // check every 5 seconds
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleRecoveringNodes() {
        for (String nodeId : nodeManager.getRecoveringNodes()) {
            NodeInfo node = nodeManager.getNodes().get(nodeId);
            if (node == null || !node.isOnline()) continue;

            System.out.println("[RecoveryService] Processing recovery for node: " + nodeId);

            // Sync all files to the recovering node
            boolean recoverySuccessful = syncNodeData(node);

            if (recoverySuccessful) {
                nodeManager.markRecovered(nodeId);
                System.out.println("[RecoveryService] Node " + nodeId + " recovery completed successfully");
            } else {
                System.out.println("[RecoveryService] Node " + nodeId + " recovery failed, will retry");
            }
        }
    }

    private void handleFailedNodes() {
        for (Map.Entry<String, NodeInfo> entry : nodeManager.getNodes().entrySet()) {
            NodeInfo node = entry.getValue();
            if (node.isOnline()) continue; // Only handle failed nodes

            System.out.println("[RecoveryService] Detected failed node: " + node.getNodeId());
        }
    }

    private boolean syncNodeData(NodeInfo targetNode) {
        boolean allFilesSynced = true;
        int syncedFiles = 0;
        int totalFiles = 0;

        System.out.println("-------------");
        System.out.println("[RecoveryService] Starting data sync for node: " + targetNode.getNodeId());
        System.out.println("---------------");

        // Get all files that should be on this node
        for (String fileName : fileManager.getAllFiles()) {
            Set<String> nodesWithFile = fileManager.getNodesForFile(fileName);
            totalFiles++;

            // If this node should have this file, ensure it's synced
            if (nodesWithFile.contains(targetNode.getNodeId())) {
                System.out.println("[RecoveryService] Syncing file: " + fileName);

                // Choose a healthy source node to sync from
                String sourceNodeId = nodesWithFile.stream()
                        .filter(id -> !id.equals(targetNode.getNodeId()) &&
                                nodeManager.getNodes().get(id).isOnline())
                        .findFirst()
                        .orElse(null);

                if (sourceNodeId == null) {
                    System.out.println("[RecoveryService]No healthy source available for file: " + fileName);
                    allFilesSynced = false;
                    continue;
                }

                NodeInfo sourceNode = nodeManager.getNodes().get(sourceNodeId);
                byte[] fileData = fileManager.getFileDataFromNode(sourceNode, fileName);

                if (fileData == null) {
                    System.out.println("[RecoveryService]Could not fetch file data for: " + fileName);
                    allFilesSynced = false;
                    continue;
                }

                // Sync file to recovering node
                try {
                    fileManager.replicateFileToTargetNode(fileName, fileData, targetNode);
                    syncedFiles++;
                    System.out.println("[RecoveryService] Synced file " + fileName + " from " + sourceNodeId + " to " + targetNode.getNodeId());
                } catch (Exception e) {
                    System.out.println("[RecoveryService] Failed to sync " + fileName + " to " + targetNode.getNodeId() + ": " + e.getMessage());
                    allFilesSynced = false;
                }
            }
        }

        System.out.println("-----------------");
        System.out.println("[RecoveryService] Sync status " + targetNode.getNodeId() + ":");
        System.out.println("  Files synced: " + syncedFiles + "/" + totalFiles);
        System.out.println("  Status: " + (allFilesSynced ? "SUCCESS" : "PARTIAL/FAILED"));
        System.out.println("---------------------");

        return allFilesSynced;
    }
}
