package com.distributed.fs;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class HeartbeatService implements Runnable {
    private final NodeManager nodeManager;
    private final FileManager fileManager;

    public HeartbeatService(NodeManager nodeManager, FileManager fileManager) {
        this.nodeManager = nodeManager;
        this.fileManager = fileManager;
    }

    @Override
    public void run() {
        try (ServerSocket serverSocket = new ServerSocket(Config.METADATA_NODE_PORT)) {
            System.out.println("Heartbeat service started on port " + Config.METADATA_NODE_PORT);
            while (true) {
                Socket socket = serverSocket.accept();
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String message = in.readLine();
                if (message != null) {
                    if (message.startsWith(Message.HEARTBEAT)) {
                        String nodeId = message.split(":")[1];
                        nodeManager.markOnline(nodeId);
                        System.out.println("Received heartbeat from " + nodeId);
                    } else if (message.startsWith(Message.FILE_UPDATE)) {
                        String[] parts = message.split(":");
                        String fileName = parts[1];
                        String nodeId = parts[2];
                        // Add file to metadata tracking
                        fileManager.addFile(fileName, nodeId);
                        System.out.println("File update: " + fileName + " on " + nodeId);
                    }
                }
                socket.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
