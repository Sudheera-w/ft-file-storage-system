package com.distributed.fs;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.util.stream.Collectors;

public class FileManager {
    private final Map<String, Set<String>> fileMap = new HashMap<>();
    private final NodeManager nodeManager;
    private static final int REPLICATION_FACTOR = 3;

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

    public byte[] getFileDataFromNode(NodeInfo sourceNode, String fileName) {
        System.out.println("Fetching file " + fileName + " from node " + sourceNode.getNodeId());
        if (fileMap.containsKey(fileName) && fileMap.get(fileName).contains(sourceNode.getNodeId())) {
            return ("File data for " + fileName + " from " + sourceNode.getNodeId()).getBytes();
        } else {
            System.out.println("Source node " + sourceNode.getNodeId() + " does not have file " + fileName);
            return null;
        }
    }

    public void replicateFileToTargetNode(String fileName, byte[] data, NodeInfo targetNode) throws IOException {
        if (targetNode == null || !targetNode.isOnline()) return;
        try (Socket socket = new Socket(targetNode.getAddress(), targetNode.getPort());
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {

            // mark as replication (not from client)
            out.writeBoolean(false);
            out.writeUTF(fileName);
            out.writeInt(data.length);
            out.write(data);

            System.out.println("Replicated " + fileName + " to " + targetNode.getNodeId());
            addFile(fileName, targetNode.getNodeId());
        }
    }

    public List<String> chooseNodesForReplication(String fileName) {
        Set<String> existingNodes = getNodesForFile(fileName);
        List<NodeInfo> onlineNodes = nodeManager.getNodes().values().stream()
                .filter(NodeInfo::isOnline)
                .filter(n -> !existingNodes.contains(n.getNodeId()))
                .collect(Collectors.toList());

        Collections.shuffle(onlineNodes);
        return onlineNodes.stream()
                .limit(REPLICATION_FACTOR - existingNodes.size())
                .map(NodeInfo::getNodeId)
                .collect(Collectors.toList());
    }

    public void ensureReplication(String fileName, byte[] fileData, String sourceNodeId) {
        Set<String> nodesWithFile = getNodesForFile(fileName);
        int needed = REPLICATION_FACTOR - nodesWithFile.size();
        if (needed <= 0) return;

        List<String> targets = chooseNodesForReplication(fileName);
        for (String targetNodeId : targets) {
            NodeInfo targetNode = nodeManager.getNodes().get(targetNodeId);
            if (targetNode == null || !targetNode.isOnline()) continue;
            try {
                replicateFileToTargetNode(fileName, fileData, targetNode);
            } catch (IOException e) {
                System.out.println("Failed to replicate " + fileName + " to " + targetNodeId);
            }
            needed--;
            if (needed == 0) break;
        }
    }
}
