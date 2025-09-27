package com.distributed.fs;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public class RaftHttpServer {
    private final RaftNode node;
    private final HttpServer server;
    private final Gson gson = new Gson();

    public RaftHttpServer(RaftNode node, int port) throws IOException {
        this.node = node;
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/requestVote", this::handleRequestVote);
        server.createContext("/appendEntries", this::handleAppendEntries);
        server.createContext("/client/put", this::handleClientPut);
        server.createContext("/client/get", this::handleClientGet);
        server.setExecutor(node.getExecutor()); // share executor
    }

    public void start() {
        server.start();
        System.out.println("HTTP server started on port " + server.getAddress().getPort());
    }

    private void handleRequestVote(HttpExchange exchange) throws IOException {
        RpcModels.RequestVoteRequest req = gson.fromJson(new InputStreamReader(exchange.getRequestBody()), RpcModels.RequestVoteRequest.class);
        RpcModels.RequestVoteResponse resp = node.onRequestVote(req);
        sendJson(exchange, resp);
    }

    private void handleAppendEntries(HttpExchange exchange) throws IOException {
        RpcModels.AppendEntriesRequest req = gson.fromJson(
                new InputStreamReader(exchange.getRequestBody()), RpcModels.AppendEntriesRequest.class);

        if (!req.entries.isEmpty()) {
            System.out.println("Received AppendEntries with " + req.entries.size() +
                    " entries on " + node.getNodeId());
        }

        RpcModels.AppendEntriesResponse resp = node.onAppendEntries(req);
        sendJson(exchange, resp);
    }


    private void handleClientPut(HttpExchange exchange) throws IOException {
        // form body: key=...&value=...
        String body = new BufferedReader(new InputStreamReader(exchange.getRequestBody()))
                .lines().collect(Collectors.joining("\n"));
        Map<String, String> params = parseForm(body);
        String key = params.get("key");
        String value = params.get("value");
        String result = node.clientPut(key, value);
        sendText(exchange, result);
    }

    private void handleClientGet(HttpExchange exchange) throws IOException {
        URI uri = exchange.getRequestURI();
        String query = uri.getQuery(); // key=...
        String key = null;
        if (query != null) {
            for (String part : query.split("&")) {
                String[] kv = part.split("=");
                if (kv.length == 2 && kv[0].equals("key")) key = kv[1];
            }
        }
        String value = node.getMetadata(key);
        if (value == null) value = "";
        sendText(exchange, value);
    }

    private Map<String,String> parseForm(String body) {
        Map<String,String> map = new HashMap<>();
        if (body == null || body.isEmpty()) return map;
        for (String kv : body.split("&")) {
            String[] parts = kv.split("=",2);
            if (parts.length==2) map.put(parts[0], parts[1]);
        }
        return map;
    }

    private void sendJson(HttpExchange exchange, Object obj) throws IOException {
        String s = gson.toJson(obj);
        byte[] bytes = s.getBytes();
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) { os.write(bytes); }
    }

    private void sendText(HttpExchange exchange, String s) throws IOException {
        byte[] bytes = s.getBytes();
        exchange.getResponseHeaders().set("Content-Type", "text/plain");
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) { os.write(bytes); }
    }
}
