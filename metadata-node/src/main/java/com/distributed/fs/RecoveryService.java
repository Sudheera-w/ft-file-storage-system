package com.distributed.fs;

import java.util.Map;

public class RecoveryService implements Runnable {
    private final NodeManager nodeManager;
    private final FileManager fileManager;

    public RecoveryService(NodeManager nodeManager, FileManager fileManager) {
        this.nodeManager = nodeManager;
        this.fileManager = fileManager;
    }

    @Override
    public void run() {
        while (true) {
            for (Map.Entry<String, NodeInfo> entry : nodeManager.getNodes().entrySet()) {
                NodeInfo node = entry.getValue();
                if (!node.isOnline()) continue;
                // Node recovered this ensures replication of its missing files
                for (String fileName : fileManager.getAllFiles()) {
                    fileManager.ensureReplication(fileName);
                }



            }
            try {
                Thread.sleep(5000); // check every 5 seconds
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


}
