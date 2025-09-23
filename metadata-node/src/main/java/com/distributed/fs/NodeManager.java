package com.distributed.fs;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NodeManager {
    private final Map<String, NodeInfo> nodes = new ConcurrentHashMap<>();
    private final Map<String, Long> lastSeen = new ConcurrentHashMap<>();
    private static final long TIMEOUT_MS = 10000; // 10 seconds

    // Register a new node in the system
    public void registerNode(String nodeId, String address) {
        nodes.putIfAbsent(nodeId, new NodeInfo(nodeId, address));
        lastSeen.put(nodeId, System.currentTimeMillis());
    }

    // Mark node as online
    public void markOnline(String nodeId) {
        NodeInfo node = nodes.get(nodeId);
        if (node != null) {
            node.setOnline(true);
            lastSeen.put(nodeId, System.currentTimeMillis());
        }
    }

    // Mark node as offline
    public void markOffline(String nodeId) {
        NodeInfo node = nodes.get(nodeId);
        if (node != null) {
            node.setOnline(false);
        }
    }

    // Update heartbeat timestamp
    public void updateHeartbeat(NodeInfo nodeInfo) {
        String nodeId = nodeInfo.getNodeId();
        lastSeen.put(nodeId, System.currentTimeMillis());
        markOnline(nodeId);
    }

    // Periodic health check
    public void checkNodeHealth() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, Long> entry : lastSeen.entrySet()) {
            String nodeId = entry.getKey();
            long lastTime = entry.getValue();

            if (now - lastTime > TIMEOUT_MS) {
                NodeInfo node = nodes.get(nodeId);
                if (node != null && node.isOnline()) {
                    System.out.println("Node " + nodeId + " considered DEAD");
                    markOffline(nodeId);
                    handleNodeFailure(nodeId);
                }
            }
        }
    }

    // Placeholder for recovery service integration
    private void handleNodeFailure(String nodeId) {
        // TODO: Trigger RecoveryService
        System.out.println("Handling failure for node " + nodeId);
    }

    // Getter for all nodes
    public Map<String, NodeInfo> getNodes() {
        return nodes;
    }
}
