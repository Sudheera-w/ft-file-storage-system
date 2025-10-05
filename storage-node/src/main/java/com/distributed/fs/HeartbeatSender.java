package com.distributed.fs;

import java.io.PrintWriter;
import java.net.Socket;

public class HeartbeatSender implements Runnable {
    private final String metadataHost;
    private final int metadataPort;
    private final String nodeId;

    public HeartbeatSender(String metadataHost, int metadataPort, String nodeId) {
        this.metadataHost = metadataHost;
        this.metadataPort = metadataPort;
        this.nodeId = nodeId;
    }

    @Override
    public void run() {
        while (true) {
            try (Socket socket = new Socket(metadataHost, metadataPort);
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                out.println(Message.HEARTBEAT + ":" + nodeId);
                Thread.sleep(Config.HEARTBEAT_INTERVAL_MS);
            } catch (Exception e) {
                System.out.println("Cannot send heartbeat: " + e.getMessage());
            }
        }
    }
}
