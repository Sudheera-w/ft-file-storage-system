package com.distributed.fs;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NodeManager {
    private final Map<String, NodeInfo> nodes = new ConcurrentHashMap<>();
    private final Map<String, Long> lastSeen = new ConcurrentHashMap<>();
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
            System.out.println("[NodeManager] Node " + nodeId + " is BACK ONLINE");
        }
        lastSeen.put(nodeId, System.currentTimeMillis());
    }

    // Mark a node offline (failed)
    public void markOffline(String nodeId) {
        NodeInfo node = nodes.get(nodeId);
        if (node != null) node.setOnline(false);
    }

    // Update heartbeat from node
    public void updateHeartbeat(NodeInfo nodeInfo) {
        lastSeen.put(nodeInfo.getNodeId(), System.currentTimeMillis());
        markOnline(nodeInfo.getNodeId());
        System.out.println("[Heartbeat] Received from " + nodeInfo.getNodeId());
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
}
