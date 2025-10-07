package com.distributed.fs.time;

import java.io.Serializable;

public class ClockedMessage<T extends Serializable> implements Serializable {
    private final T payload;
    private final long lamport;
    private final String fromNode;

    public ClockedMessage(T payload, long lamport, String fromNode) {
        this.payload = payload;
        this.lamport = lamport;
        this.fromNode = fromNode;
    }

    public T getPayload() { return payload; }
    public long getLamport() { return lamport; }
    public String getFromNode() { return fromNode; }
}
