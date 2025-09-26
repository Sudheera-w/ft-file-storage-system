package com.distributed.fs;

import java.util.*;
import java.util.stream.Collectors;

public class FileManager {
    private final Map<String, Set<String>> fileMap = new HashMap<>(); // fileName -> set of nodeIds
    private final NodeManager nodeManager;
    private static final int REPLICATION_FACTOR = 3;

    public FileManager(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
    }

    // Add a file to a node
    public void addFile(String fileName, String nodeId) {
        fileMap.putIfAbsent(fileName, new HashSet<>());
        fileMap.get(fileName).add(nodeId);
    }

    // Get all registered files
    public Iterable<String> getAllFiles() {
        return fileMap.keySet();
    }

    // Get nodes storing a specific file
    public Set<String> getNodesForFile(String fileName) {
        return fileMap.getOrDefault(fileName, new HashSet<>());
    }

    // Choose nodes for replication (excluding nodes that already have it)
    public List<String> chooseNodesForReplication(String fileName) {
        Set<String> existingNodes = getNodesForFile(fileName);
        List<NodeInfo> onlineNodes = nodeManager.getNodes().values().stream()
                .filter(NodeInfo::isOnline)
                .filter(n -> !existingNodes.contains(n.getNodeId()))
                .collect(Collectors.toList());

        Collections.shuffle(onlineNodes); // random selection
        List<String> chosen = onlineNodes.stream()
                .limit(REPLICATION_FACTOR - existingNodes.size())
                .map(NodeInfo::getNodeId)
                .collect(Collectors.toList());

        return chosen;
    }

    // Ensure replication factor is met
    public void ensureReplication(String fileName) {
        Set<String> nodesWithFile = getNodesForFile(fileName);
        int needed = REPLICATION_FACTOR - nodesWithFile.size();
        if (needed <= 0) return;

        List<String> newNodes = chooseNodesForReplication(fileName);
        for (String nodeId : newNodes) {
            System.out.println("Replicating " + fileName + " to " + nodeId);
            nodesWithFile.add(nodeId);
            // Here,  trigger the actual replication via StorageNode/RecoveryService
        }
    }
}
