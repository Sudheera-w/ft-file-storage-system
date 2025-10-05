package com.distributed.fs;

public class NodeInfo {
    private final String nodeId;
    private final String address;
    private final int port;
    private boolean online;

    public NodeInfo(String nodeId, String address, int port) {
        this.nodeId = nodeId;
        this.address = address;
        this.port = port;
        this.online = true;
    }

    public String getNodeId() { return nodeId; }
    public String getAddress() { return address; }
    public int getPort() { return port; }
    public boolean isOnline() { return online; }
    public void setOnline(boolean online) { this.online = online; }
}
