package com.distributed.fs.time;

public class TimeSyncManager {
    private final LamportClock clock;

    public TimeSyncManager(String nodeId) {
        this.clock = new LamportClock(nodeId);
    }

    public long nextEvent() {
        return clock.sendEvent();
    }

    public void syncFromMessage(long receivedTime) {
        clock.receiveEvent(receivedTime);
    }

    public long getTime() {
        return clock.get();
    }

    public String getNodeId() {
        return clock.nodeId();
    }
}
