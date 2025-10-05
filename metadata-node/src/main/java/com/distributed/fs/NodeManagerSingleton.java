package com.distributed.fs;

public class NodeManagerSingleton {
    private static NodeManager instance;

    private NodeManagerSingleton() {}

    public static NodeManager getInstance() {
        if (instance == null) {
            instance = new NodeManager();
        }
        return instance;
    }
}
