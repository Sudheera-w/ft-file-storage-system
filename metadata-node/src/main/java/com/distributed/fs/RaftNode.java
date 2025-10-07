package com.distributed.fs;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RaftNode {
    enum Role { FOLLOWER, CANDIDATE, LEADER }

    private final String nodeId;               // e.g. "localhost:8001"
    private final List<String> peers;          // peer ids
    private final int port;

    // Raft persistent state (in-memory for this prototype)
    private int currentTerm = 0;
    private String votedFor = null;
    private final List<LogEntry> log = new ArrayList<>(); // index 0 unused for simplicity

    // volatile state
    private int commitIndex = 0;
    private int lastApplied = 0;

    // leader state
    private final Map<String,Integer> nextIndex = new ConcurrentHashMap<>();
    private final Map<String,Integer> matchIndex = new ConcurrentHashMap<>();

    private Role role = Role.FOLLOWER;
    private String leaderId = null;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
    private final ExecutorService executor = Executors.newFixedThreadPool(8);
    private final RaftClient client = new RaftClient();
    private final MetadataStateMachine stateMachine = new MetadataStateMachine();

    private final Random rand = new Random();
    private ScheduledFuture<?> electionTimeoutFuture;
    private ScheduledFuture<?> heartbeatFuture;

    private final Object lock = new Object(); // protects Raft state

    public RaftNode(String nodeId, List<String> peers, int port) {
        this.nodeId = nodeId;
        this.peers = new ArrayList<>(peers);
        this.port = port;
        // to make log indexing simple, insert a dummy entry at index 0
        log.add(new LogEntry(0, ""));
    }

    public String getNodeId() {
        return nodeId;
    }


    public ExecutorService getExecutor() { return executor; }

    public void start() throws Exception {
        RaftHttpServer http = new RaftHttpServer(this, port);
        http.start();
        resetElectionTimeout();
    }

    // --- Election timeout & leader heartbeat management ---
    private void resetElectionTimeout() {
        if (electionTimeoutFuture != null) electionTimeoutFuture.cancel(true);
        int timeout = 300 + rand.nextInt(200); // 300-500ms for demo
        electionTimeoutFuture = scheduler.schedule(this::onElectionTimeout, timeout, TimeUnit.MILLISECONDS);
    }

    private void onElectionTimeout() {
        synchronized (lock) {
            if (role == Role.LEADER) return; // leader won't start election
            role = Role.CANDIDATE;
            currentTerm += 1;
            votedFor = nodeId;
            System.out.println(nodeId + " -> starting election for term " + currentTerm);
        }
        startElection();
    }

    private void startElection() {
        final int termStarted;
        synchronized (lock) { termStarted = currentTerm; }
        AtomicInteger votes = new AtomicInteger(1); // vote for self
        CountDownLatch latch = new CountDownLatch(peers.size());
        for (String peer : peers) {
            scheduler.submit(() -> {
                try {
                    RpcModels.RequestVoteRequest req = new RpcModels.RequestVoteRequest();
                    req.term = termStarted;
                    req.candidateId = nodeId;
                    synchronized (lock) {
                        req.lastLogIndex = log.size()-1;
                        req.lastLogTerm = (req.lastLogIndex >= 0) ? log.get(req.lastLogIndex).term : 0;
                    }
                    RpcModels.RequestVoteResponse resp = client.requestVote(peer, req);
                    if (resp != null) {
                        synchronized (lock) {
                            if (resp.term > currentTerm) {
                                currentTerm = resp.term;
                                role = Role.FOLLOWER;
                                votedFor = null;
                            }
                        }
                        if (resp.voteGranted && role == Role.CANDIDATE && currentTerm == termStarted) {
                            int v = votes.incrementAndGet();
                            if (v > peers.size() / 2) {
                                becomeLeader();
                            }
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        // wait short while for replies, but election may still continue
        try { latch.await(400, TimeUnit.MILLISECONDS); } catch (InterruptedException ignored) {}
        // reset election timeout if still follower/candidate
        resetElectionTimeout();
    }

    private void becomeLeader() {
        synchronized (lock) {
            role = Role.LEADER;
            leaderId = nodeId;
            System.out.println(nodeId + " BECAME LEADER for term " + currentTerm);
            // init leader state
            int next = log.size();
            for (String p : peers) {
                nextIndex.put(p, next);
                matchIndex.put(p, 0);
            }
            if (heartbeatFuture != null) heartbeatFuture.cancel(true);
            heartbeatFuture = scheduler.scheduleAtFixedRate(this::sendHeartbeats, 0, 150, TimeUnit.MILLISECONDS);
        }
    }

    private void stepDownIfTermHigher(int term) {
        synchronized (lock) {
            if (term > currentTerm) {
                currentTerm = term;
                role = Role.FOLLOWER;
                votedFor = null;
                leaderId = null;
                if (heartbeatFuture != null) heartbeatFuture.cancel(true);
                resetElectionTimeout();
            }
        }
    }

    private void sendHeartbeats() {
        synchronized (lock) {
            if (role != Role.LEADER) return;
        }
        for (String peer : peers) {
            scheduler.execute(() -> {
                RpcModels.AppendEntriesRequest req = buildAppendEntriesRequestForPeer(peer);
                RpcModels.AppendEntriesResponse resp = client.appendEntries(peer, req);
                if (resp != null) {
                    if (resp.term > currentTerm) {
                        stepDownIfTermHigher(resp.term);
                    } else {
                        if (resp.success) {
                            // update nextIndex/matchIndex - simplified:
                            int lastIndex;
                            synchronized (lock) { lastIndex = log.size()-1; }
                            matchIndex.put(peer, lastIndex);
                            nextIndex.put(peer, lastIndex + 1);
                            advanceCommitIndex();
                        } else {
                            // follower mismatch, decrement nextIndex and retry next heartbeat
                            nextIndex.merge(peer, 1, (oldv, one) -> Math.max(1, oldv - 1));
                        }
                    }
                }
            });
        }
    }

    private RpcModels.AppendEntriesRequest buildAppendEntriesRequestForPeer(String peer) {
        RpcModels.AppendEntriesRequest req = new RpcModels.AppendEntriesRequest();
        synchronized (lock) {
            req.term = currentTerm;
            req.leaderId = nodeId;
            int nextIdx = nextIndex.getOrDefault(peer, log.size());
            int prevIdx = Math.max(0, nextIdx - 1);
            req.prevLogIndex = prevIdx;
            req.prevLogTerm = (prevIdx >= 0 && prevIdx < log.size()) ? log.get(prevIdx).term : 0;
            // send entries from nextIdx to end
            List<LogEntry> entries = new ArrayList<>();
            for (int i = nextIdx; i < log.size(); i++) entries.add(log.get(i));
            req.entries = entries;
            req.leaderCommit = commitIndex;
        }
        return req;
    }

    // try to update commitIndex based on matchIndex of followers (simplified)
    private void advanceCommitIndex() {
        synchronized (lock) {
            int N = log.size() - 1;
            for (int idx = commitIndex + 1; idx <= N; idx++) {
                int count = 1; // leader itself
                for (String p : peers) {
                    Integer m = matchIndex.get(p);
                    if (m != null && m >= idx) count++;
                }
                if (count > peers.size() / 2 && log.get(idx).term == currentTerm) {
                    commitIndex = idx;
                }
            }
        }
        applyCommitted();
    }

    private void applyCommitted() {
        synchronized (lock) {
            while (lastApplied < commitIndex) {
                lastApplied++;
                LogEntry entry = log.get(lastApplied);
                stateMachine.apply(entry.command);
            }
        }
    }

    // --- RPC Handlers ---

    // called by RaftHttpServer when /requestVote arrives
    public RpcModels.RequestVoteResponse onRequestVote(RpcModels.RequestVoteRequest req) {
        RpcModels.RequestVoteResponse resp = new RpcModels.RequestVoteResponse();
        synchronized (lock) {
            if (req.term < currentTerm) {
                resp.term = currentTerm;
                resp.voteGranted = false;
                return resp;
            }
            // If candidate's term is up to date, grant if we haven't voted or voted for candidate
            if (req.term > currentTerm) {
                currentTerm = req.term;
                votedFor = null;
                role = Role.FOLLOWER;
            }
            boolean logOk = true;
            int lastIndex = log.size() - 1;
            int lastTerm = (lastIndex >= 0) ? log.get(lastIndex).term : 0;
            if (req.lastLogTerm < lastTerm) logOk = false;
            else if (req.lastLogTerm == lastTerm && req.lastLogIndex < lastIndex) logOk = false;

            if ((votedFor == null || votedFor.equals(req.candidateId)) && logOk) {
                votedFor = req.candidateId;
                resp.voteGranted = true;
                resetElectionTimeout();
            } else {
                resp.voteGranted = false;
            }
            resp.term = currentTerm;
        }
        return resp;
    }

    // called by RaftHttpServer when /appendEntries arrives
    public RpcModels.AppendEntriesResponse onAppendEntries(RpcModels.AppendEntriesRequest req) {
        RpcModels.AppendEntriesResponse resp = new RpcModels.AppendEntriesResponse();
        synchronized (lock) {
            if (req.term < currentTerm) {
                resp.term = currentTerm;
                resp.success = false;
                return resp;
            }
            // become follower for leader's term
            leaderId = req.leaderId;
            if (req.term > currentTerm) { currentTerm = req.term; votedFor = null; }
            role = Role.FOLLOWER;
            resetElectionTimeout();

            // Check if log contains entry at prevLogIndex with term prevLogTerm
            if (req.prevLogIndex >= log.size() || log.get(req.prevLogIndex).term != req.prevLogTerm) {
                resp.term = currentTerm;
                resp.success = false;
                return resp;
            }
            // Append any new entries (delete conflicts)
            int idx = req.prevLogIndex + 1;
            int i = 0;
            while (i < req.entries.size()) {
                if (idx < log.size()) {
                    if (log.get(idx).term != req.entries.get(i).term) {
                        // delete conflict and append the rest
                        while (log.size() > idx) log.remove(log.size()-1);
                        break;
                    }
                }
                idx++; i++;
            }
            // append remaining
            while (i < req.entries.size()) {
                log.add(req.entries.get(i));
                i++;
            }
            // update commit index
            if (req.leaderCommit > commitIndex) {
                commitIndex = Math.min(req.leaderCommit, log.size()-1);
                applyCommitted();
            }
            resp.term = currentTerm;
            resp.success = true;
        }
        return resp;
    }

    // client-facing: attempt to put metadata (key->value)
    // If not leader, returns "NOT_LEADER:<leaderId>" so client can retry on leader
    public String clientPut(String key, String value) {
        synchronized (lock) {
            if (role != Role.LEADER) {
                String leader = leaderId == null ? "" : leaderId;
                return "NOT_LEADER:" + leader;
            }
            // append to own log
            LogEntry entry = new LogEntry(currentTerm, "PUT " + key + " " + value);
            log.add(entry);
            int index = log.size() - 1;
            // after appending locally, leader will try to replicate in background via heartbeats/appendEntries
            // For simplicity in this prototype we will attempt to replicate to peers synchronously and wait for acks
        }
        // naive sync replication: try to ask peers and count acks
        int successes = 1; // leader itself
        CountDownLatch latch = new CountDownLatch(peers.size());
        for (String peer : peers) {
            scheduler.submit(() -> {
                RpcModels.AppendEntriesRequest req = buildAppendEntriesRequestForPeer(peer);
                RpcModels.AppendEntriesResponse r = client.appendEntries(peer, req);
                if (r != null && r.success) {
                    synchronized (lock) {
                        matchIndex.put(peer, log.size() - 1);
                    }
                }
            });
        }

        try { latch.await(300, TimeUnit.MILLISECONDS); } catch (InterruptedException ignored) {}
        // quick check: recompute how many have this index
        synchronized (lock) {
            int index = log.size() - 1;
            int count = 1;
            for (String p : peers) {
                Integer m = matchIndex.get(p);
                if (m != null && m >= index) count++;
            }
            if (count > peers.size() / 2) {
                // committed
                commitIndex = index;
                applyCommitted();

                // NEW: notify followers with updated commit index
                for (String peer : peers) {
                    scheduler.submit(() -> {
                        RpcModels.AppendEntriesRequest req = buildAppendEntriesRequestForPeer(peer);
                        client.appendEntries(peer, req);
                    });
                }

                return "OK";
            } else {
                return "IN_PROGRESS";
            }

        }
    }

    public String getMetadata(String key) {
        return stateMachine.get(key);
    }
}
