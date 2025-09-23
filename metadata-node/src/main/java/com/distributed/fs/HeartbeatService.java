package com.distributed.fs;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class HeartbeatService implements Runnable {
    private final NodeManager nodeManager;

    public HeartbeatService(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
    }

    @Override
    public void run() {
        try (ServerSocket serverSocket = new ServerSocket(Config.METADATA_NODE_PORT)) {
            System.out.println("Heartbeat service started on port " + Config.METADATA_NODE_PORT);
            while (true) {
                Socket socket = serverSocket.accept();
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String message = in.readLine();
                if (message != null && message.startsWith(Message.HEARTBEAT)) {
                    String nodeId = message.split(":")[1];
                    nodeManager.markOnline(nodeId);
                    System.out.println("Received heartbeat from " + nodeId);
                }
                socket.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
