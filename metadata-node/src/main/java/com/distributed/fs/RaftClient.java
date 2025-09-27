package com.distributed.fs;

import com.google.gson.Gson;

import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class RaftClient {
    private final Gson gson = new Gson();

    public RpcModels.RequestVoteResponse requestVote(String peer, RpcModels.RequestVoteRequest req) {
        try {
            URL url = new URL("http://" + peer + "/requestVote");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("POST");
            con.setDoOutput(true);
            con.setRequestProperty("Content-Type", "application/json");
            String body = gson.toJson(req);
            try (OutputStream os = con.getOutputStream()) { os.write(body.getBytes()); }
            try (InputStreamReader reader = new InputStreamReader(con.getInputStream())) {
                return gson.fromJson(reader, RpcModels.RequestVoteResponse.class);
            }
        } catch (Exception e) {
            // peer unreachable -> treat as no response
            return null;
        }
    }

    public RpcModels.AppendEntriesResponse appendEntries(String peer, RpcModels.AppendEntriesRequest req) {
        try {
            URL url = new URL("http://" + peer + "/appendEntries");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("POST");
            con.setDoOutput(true);
            con.setRequestProperty("Content-Type", "application/json");
            String body = gson.toJson(req);
            try (OutputStream os = con.getOutputStream()) {
                os.write(body.getBytes());
                os.flush();}
            try (InputStreamReader reader = new InputStreamReader(con.getInputStream())) {
                return gson.fromJson(reader, RpcModels.AppendEntriesResponse.class);
            }
        } catch (Exception e) {
            return null;
        }
    }

    public String forwardClientPut(String peer, String key, String value) {
        try {
            URL url = new URL("http://" + peer + "/client/put");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("POST");
            con.setDoOutput(true);
            String body = "key=" + key + "&value=" + value;
            con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            try (OutputStream os = con.getOutputStream()) { os.write(body.getBytes()); }
            try (InputStreamReader reader = new InputStreamReader(con.getInputStream())) {
                StringBuilder sb = new StringBuilder();
                int c;
                while ((c = reader.read()) != -1) sb.append((char) c);
                return sb.toString();
            }
        } catch (Exception e) {
            return "error: " + e.getMessage();
        }
    }
}
