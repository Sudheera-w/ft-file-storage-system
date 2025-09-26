package com.distributed.fs;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NodeManager {
    private final Map<String, NodeInfo> nodes = new ConcurrentHashMap<>();
    private final Map<String, Long> lastSeen = new ConcurrentHashMap<>();
    private static final long TIMEOUT_MS = 10000; // 10 seconds

    public void registerNode(String nodeId, String address, int port) {
        nodes.putIfAbsent(nodeId, new NodeInfo(nodeId, address, port));
        lastSeen.put(nodeId, System.currentTimeMillis());
    }

    public void markOnline(String nodeId) {
        NodeInfo node = nodes.get(nodeId);
        if (node != null) {
            node.setOnline(true);
            lastSeen.put(nodeId, System.currentTimeMillis());
        }
    }

    public void markOffline(String nodeId) {
        NodeInfo node = nodes.get(nodeId);
        if (node != null) node.setOnline(false);
    }

    public void updateHeartbeat(NodeInfo nodeInfo) {
        lastSeen.put(nodeInfo.getNodeId(), System.currentTimeMillis());
        markOnline(nodeInfo.getNodeId());
    }

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

    private void handleNodeFailure(String nodeId) {
        System.out.println("Handling failure for node " + nodeId);
    }

    public Map<String, NodeInfo> getNodes() {
        return nodes;
    }
}
