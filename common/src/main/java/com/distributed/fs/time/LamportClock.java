package com.distributed.fs.time;

import java.util.concurrent.atomic.AtomicLong;

public class LamportClock {
    private final AtomicLong time = new AtomicLong(0);
    private final String nodeId;

    public LamportClock(String nodeId) {
        this.nodeId = nodeId;
    }

    public synchronized long tick() {
        return time.incrementAndGet();
    }

    public synchronized long sendEvent() {
        return time.incrementAndGet();
    }

    public synchronized long receiveEvent(long incomingTs) {
        long cur = time.get();
        long next = Math.max(cur, incomingTs) + 1;
        time.set(next);
        return next;
    }

    public long get() {
        return time.get();
    }

    public String nodeId() {
        return nodeId;
    }
}
