package com.distributed.fs;

import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {
        // Usage: --port=8001 --peers=localhost:8002,localhost:8003
        int port = 8001;
        String peersArg = "";
        for (String arg : args) {
            if (arg.startsWith("--port=")) port = Integer.parseInt(arg.substring("--port=".length()));
            if (arg.startsWith("--peers=")) peersArg = arg.substring("--peers=".length());
        }
        List<String> peers = peersArg.isEmpty() ? List.of() : Arrays.asList(peersArg.split(","));
        String myId = "localhost:" + port;

        System.out.println("Starting metadata node " + myId + " with peers " + peers);
        RaftNode node = new RaftNode(myId, peers, port);
        node.start();
        // keep running
        Thread.currentThread().join();
    }
}
