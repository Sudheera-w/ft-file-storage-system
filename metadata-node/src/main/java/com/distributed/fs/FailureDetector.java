package com.distributed.fs;

public class FailureDetector implements Runnable {
    private final NodeManager nodeManager;

    public FailureDetector(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
    }

    @Override
    public void run() {
        while (true) {
            try {
                System.out.println("[FailureDetector] Running health check...");
                nodeManager.checkNodeHealth();
                Thread.sleep(2000); // check every 2 seconds
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
