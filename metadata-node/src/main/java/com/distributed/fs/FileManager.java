package com.distributed.fs;

import java.util.*;

public class FileManager {
    private final Map<String, Set<String>> fileMap = new HashMap<>(); // fileName --> set of nodeIds
    private final NodeManager nodeManager;

    public FileManager(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
    }

    public void addFile(String fileName, String nodeId) {
        fileMap.putIfAbsent(fileName, new HashSet<>());
        fileMap.get(fileName).add(nodeId);
    }
    public Iterable<String> getAllFiles() {
        return fileMap.keySet();
    }


    public Set<String> getNodesForFile(String fileName) {
        return fileMap.getOrDefault(fileName, new HashSet<>());
    }

    public void ensureReplication(String fileName) {
        Set<String> nodesWithFile = getNodesForFile(fileName);
        int needed = Config.REPLICATION_FACTOR - nodesWithFile.size();
        if (needed <= 0) return;

        // Pick online nodes without the file
        for (NodeInfo node : nodeManager.getNodes().values()) {
            if (!nodesWithFile.contains(node.getNodeId()) && node.isOnline()) {
                nodesWithFile.add(node.getNodeId());
                System.out.println("Replicating " + fileName + " to " + node.getNodeId());
                needed--;
                if (needed == 0) break;
            }
        }
    }
}
