package com.distributed.fs.time;

public class LamportClock {
    private long time = 0;
    private final String nodeId;

    public LamportClock(String nodeId) {
        this.nodeId = nodeId;
    }

    public synchronized long sendEvent() {
        time++;
        return time;
    }

    public synchronized void receiveEvent(long receivedTime) {
        time = Math.max(time, receivedTime) + 1;
    }

    public synchronized long get() {
        return time;
    }

    public String nodeId() {
        return nodeId;
    }
}
