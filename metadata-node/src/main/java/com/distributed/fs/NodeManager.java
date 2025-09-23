package com.distributed.fs;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NodeManager {
    private final Map<String, NodeInfo> nodes = new ConcurrentHashMap<>();

    public void registerNode(String nodeId, String address) {
        nodes.putIfAbsent(nodeId, new NodeInfo(nodeId, address));
    }

    public void markOnline(String nodeId) {
        NodeInfo node = nodes.get(nodeId);
        if (node != null) node.setOnline(true);
    }

    public void markOffline(String nodeId) {
        NodeInfo node = nodes.get(nodeId);
        if (node != null) node.setOnline(false);
    }

    public Map<String, NodeInfo> getNodes() {
        return nodes;
    }
}
