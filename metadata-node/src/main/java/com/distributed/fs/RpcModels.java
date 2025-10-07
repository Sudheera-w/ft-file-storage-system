package com.distributed.fs;

import java.util.List;

public class RpcModels {
    public static class RequestVoteRequest {
        public int term;
        public String candidateId;
        public int lastLogIndex;
        public int lastLogTerm;
    }
    public static class RequestVoteResponse {
        public int term;
        public boolean voteGranted;
    }

    public static class AppendEntriesRequest {
        public int term;
        public String leaderId;
        public int prevLogIndex;
        public int prevLogTerm;
        public List<LogEntry> entries; // new entries (possibly empty for heartbeat)
        public int leaderCommit;
    }
    public static class AppendEntriesResponse {
        public int term;
        public boolean success;
    }
}
