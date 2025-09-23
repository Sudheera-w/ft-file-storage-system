package com.distributed.fs;

import java.util.HashSet;
import java.util.Set;

public class NodeInfo {
    private String nodeId;
    private String address;
    private boolean online;
    private Set<String> files;

    public NodeInfo(String nodeId, String address) {
        this.nodeId = nodeId;
        this.address = address;
        this.online = true;
        this.files = new HashSet<>();
    }

    public String getNodeId() { return nodeId; }
    public String getAddress() { return address; }
    public boolean isOnline() { return online; }
    public void setOnline(boolean online) { this.online = online; }
    public Set<String> getFiles() { return files; }
}
