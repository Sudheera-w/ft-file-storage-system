package com.distributed.fs;

public class Config {
    public static final int HEARTBEAT_INTERVAL_MS = 3000;   // heartbeat every 3s
    public static final int HEARTBEAT_TIMEOUT_MS = 10000;   // consider node offline after 10s
    public static final int REPLICATION_FACTOR = 3;         // number of copies
    public static final int STORAGE_NODE_PORT = 9100;
    public static final int METADATA_NODE_PORT = 5000;
}
