package com.distributed.fs;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

public class NodeManager {
    private final Map<String, NodeInfo> nodes = new ConcurrentHashMap<>();
    private final Map<String, Long> lastSeen = new ConcurrentHashMap<>();
    private final Set<String> recoveringNodes = ConcurrentHashMap.newKeySet();
    private static final long TIMEOUT_MS = 10000; // 10 seconds

    // Register a node
    public void registerNode(String nodeId, String address, int port) {
        nodes.putIfAbsent(nodeId, new NodeInfo(nodeId, address, port));
        NodeInfo node = nodes.get(nodeId);
        node.setOnline(false); // default offline
    }

    // Mark a node online when heartbeat received
    public void markOnline(String nodeId) {
        NodeInfo node = nodes.get(nodeId);
        if (node != null && !node.isOnline()) {
            node.setOnline(true);
            recoveringNodes.add(nodeId);
            System.out.println("[NodeManager] Node " + nodeId + " is BACK ONLINE - Starting recovery process");
        }
        lastSeen.put(nodeId, System.currentTimeMillis());
    }

    // Mark a node offline (failed)
    public void markOffline(String nodeId) {
        NodeInfo node = nodes.get(nodeId);
        if (node != null) node.setOnline(false);
    }

    // Check node health periodically
    public void checkNodeHealth() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, NodeInfo> entry : nodes.entrySet()) {
            String nodeId = entry.getKey();
            NodeInfo node = entry.getValue();
            Long lastTime = lastSeen.get(nodeId); // may be null

            System.out.println("[HealthCheck] Node " + nodeId +
                    ", online=" + node.isOnline() +
                    ", lastSeen=" + lastTime);

            if (lastTime == null || now - lastTime > TIMEOUT_MS) {
                if (node.isOnline()) {
                    System.out.println("Node " + nodeId + " considered DEAD");
                    markOffline(nodeId);
                    handleNodeFailure(nodeId);
                }
            }
        }
    }

    // Handle failure (can trigger recovery)
    private void handleNodeFailure(String nodeId) {
        System.out.println("[NodeManager] Handling failure for node " + nodeId);
    }

    public Map<String, NodeInfo> getNodes() {
        return nodes;
    }

    // Get nodes that are currently recovering
    public Set<String> getRecoveringNodes() {
        return new HashSet<>(recoveringNodes);
    }

    // Mark a node as fully recovered
    public void markRecovered(String nodeId) {
        recoveringNodes.remove(nodeId);
        System.out.println("[NodeManager] Node " + nodeId + " recovery completed");
    }
}
