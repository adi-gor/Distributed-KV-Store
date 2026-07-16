package com.aditya.kvstore.raft;

import com.aditya.kvstore.store.KeyValueStore;

import java.io.*;
import java.net.Socket;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class RaftNode {

    private enum State { FOLLOWER, CANDIDATE, LEADER }

    private static class LogEntry {
        final long term;
        final long index;
        final String command;
        LogEntry(long term, long index, String command) {
            this.term = term;
            this.index = index;
            this.command = command;
        }
    }

    // --- Tunables ---
    private static final long ELECTION_TIMEOUT_MIN_MS = 150;
    private static final long ELECTION_TIMEOUT_MAX_MS = 300;
    private static final long HEARTBEAT_INTERVAL_MS = 50;
    private static final long ELECTION_POLL_INTERVAL_MS = 10;
    private static final long APPLY_POLL_INTERVAL_MS = 10;
    private static final int RPC_TIMEOUT_MS = 100;
    private static final long SUBMIT_COMMIT_TIMEOUT_MS = 2000;

    private static final long PERSIST_BATCH_INTERVAL_MS = 5;
    private static final int STORE_MAX_ENTRIES = 2000;

    private static final Pattern TOKEN_PATTERN = Pattern.compile("\"([^\"]*)\"|(\\S+)");

    private final int nodeId;
    private final int ownPort;
    private final int clientPort;
    private final List<String> peers;
    private final File headerFile;
    private final File logFile;
    private long persistedLogSize = 0;


    private final KeyValueStore store = new KeyValueStore(STORE_MAX_ENTRIES);


    private final Object stateLock = new Object();
    private long currentTerm = 0;
    private Integer votedFor = null;
    private State state = State.FOLLOWER;
    private Integer leaderId = null;
    private long lastContactTime = System.currentTimeMillis();
    private long currentTimeoutMs;


    private final List<LogEntry> log = new ArrayList<>();
    private long commitIndex = 0;
    private long lastApplied = 0;


    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();


    private final Map<String, Boolean> peerInFlight = new ConcurrentHashMap<>();

    private final Map<Long, CountDownLatch> pendingSubmissions = new HashMap<>();

    private final Consumer<LogEntry> applyCallback = entry -> applyCommandToStore(entry.command);


    private final Object persistLock = new Object();
    private boolean persistDirty = false;
    private CountDownLatch currentPersistBatch = new CountDownLatch(1);


    private final ExecutorService controlPool = Executors.newCachedThreadPool();


    private final ExecutorService clientPool = Executors.newCachedThreadPool();
    private volatile boolean running = true;

    public RaftNode(int nodeId, int ownPort, int clientPort, List<String> peers) {
        this.nodeId = nodeId;
        this.ownPort = ownPort;
        this.clientPort = clientPort;
        this.peers = peers;
        this.currentTimeoutMs = randomElectionTimeout();

        File dir = new File("data/raft");
        dir.mkdirs();
        this.headerFile = new File(dir, "raft-node-" + nodeId + ".header");
        this.logFile = new File(dir, "raft-node-" + nodeId + ".log");

        loadPersistedState();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: RaftNode <nodeId> <raftPort> <clientPort> <peerRaftHost:peerRaftPort> [more peers...]");
            return;
        }
        int nodeId = Integer.parseInt(args[0]);
        int ownPort = Integer.parseInt(args[1]);
        int clientPort = Integer.parseInt(args[2]);
        List<String> peers = new ArrayList<>();
        for (int i = 3; i < args.length; i++) peers.add(args[i]);

        RaftNode node = new RaftNode(nodeId, ownPort, clientPort, peers);
        node.start();
    }

    public void start() {
        log("Starting as FOLLOWER. Peers: " + peers);
        controlPool.submit(this::runRpcServer);
        clientPool.submit(this::runClientServer);
        controlPool.submit(this::electionTimerLoop);
        controlPool.submit(this::heartbeatLoop);
        controlPool.submit(this::applyLoop);
        controlPool.submit(this::persistFlusherLoop);
    }

    // ================= RPC SERVER =================

    private void runRpcServer() {
        try (ServerSocket serverSocket = new ServerSocket(ownPort)) {
            log("RPC server listening on port " + ownPort);
            while (running) {
                Socket socket = serverSocket.accept();
                controlPool.submit(() -> handleConnection(socket));
            }
        } catch (IOException e) {
            log("RPC server error: " + e.getMessage());
        }
    }

    private void handleConnection(Socket socket) {
        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                OutputStream out = socket.getOutputStream()
        ) {
            String headerLine = in.readLine();
            if (headerLine == null) return;
            String response = dispatchRpc(headerLine, in);
            out.write((response + "\n").getBytes());
            out.flush();
        } catch (IOException e) {
            // Dropped connections are normal/expected in a distributed system.
        } finally {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    private String dispatchRpc(String headerLine, BufferedReader in) throws IOException {
        String[] parts = headerLine.trim().split("\\s+");
        String rpcType = parts[0];

        switch (rpcType) {
            case "REQUEST_VOTE": {
                long term = Long.parseLong(parts[1]);
                int candidateId = Integer.parseInt(parts[2]);
                long lastLogIndex = Long.parseLong(parts[3]);
                long lastLogTerm = Long.parseLong(parts[4]);
                return handleRequestVote(term, candidateId, lastLogIndex, lastLogTerm);
            }
            case "APPEND_ENTRIES": {
                long term = Long.parseLong(parts[1]);
                int leaderIdParam = Integer.parseInt(parts[2]);
                long prevLogIndex = Long.parseLong(parts[3]);
                long prevLogTerm = Long.parseLong(parts[4]);
                long leaderCommit = Long.parseLong(parts[5]);
                int entryCount = Integer.parseInt(parts[6]);

                List<LogEntry> entries = new ArrayList<>();
                for (int i = 0; i < entryCount; i++) {
                    String meta = in.readLine();
                    String command = in.readLine();
                    String[] metaParts = meta.trim().split("\\s+");
                    entries.add(new LogEntry(Long.parseLong(metaParts[0]), Long.parseLong(metaParts[1]), command));
                }
                return handleAppendEntries(term, leaderIdParam, prevLogIndex, prevLogTerm, leaderCommit, entries);
            }
            case "SUBMIT_COMMAND": {
                String command = in.readLine();
                return handleSubmit(command);
            }
            case "STATUS": {
                synchronized (stateLock) {
                    return "state=" + state + " term=" + currentTerm + " commitIndex=" + commitIndex
                            + " logSize=" + log.size() + " leaderId=" + leaderId;
                }
            }
            default:
                return "ERR unknown_rpc";
        }
    }

    // ================= RPC HANDLERS (receiving side) =================

    private String handleRequestVote(long term, int candidateId, long lastLogIndex, long lastLogTerm) {
        long responseTerm;
        boolean voteGranted;
        CountDownLatch latchToAwait = null;

        synchronized (stateLock) {
            long termBefore = currentTerm;

            if (term < currentTerm) {
                return currentTerm + " false"; // no state change -- nothing to persist
            }
            if (term > currentTerm) {
                stepDownToFollower(term);
            }

            boolean canVote = (votedFor == null || votedFor == candidateId);
            LogEntry myLast = log.get(log.size() - 1);
            boolean candidateLogOk = (lastLogTerm > myLast.term)
                    || (lastLogTerm == myLast.term && lastLogIndex >= myLast.index);

            if (canVote && candidateLogOk) {
                votedFor = candidateId;
                resetElectionTimer();

                latchToAwait = requestPersist();
                voteGranted = true;
                log("Granted vote to node " + candidateId + " for term " + term);
            } else {
                if (currentTerm != termBefore) latchToAwait = requestPersist();
                voteGranted = false;
            }
            responseTerm = currentTerm;
        }

        if (latchToAwait != null) awaitPersist(latchToAwait);
        return responseTerm + " " + voteGranted;
    }

    private String handleAppendEntries(long term, int leaderIdParam, long prevLogIndex, long prevLogTerm,
                                       long leaderCommit, List<LogEntry> entries) {
        long responseTerm;
        boolean success;
        CountDownLatch latchToAwait = null;

        synchronized (stateLock) {
            long termBefore = currentTerm;

            if (term < currentTerm) {
                return currentTerm + " false";
            }
            if (term > currentTerm) {
                stepDownToFollower(term);
            } else if (state != State.FOLLOWER) {
                state = State.FOLLOWER;
            }

            leaderId = leaderIdParam;
            resetElectionTimer();

            if (prevLogIndex >= log.size() || log.get((int) prevLogIndex).term != prevLogTerm) {
                success = false; // leader will backtrack nextIndex and retry
            } else {
                int insertPos = (int) prevLogIndex + 1;
                for (LogEntry newEntry : entries) {
                    if (insertPos < log.size()) {
                        if (log.get(insertPos).term != newEntry.term) {
                            while (log.size() > insertPos) log.remove(log.size() - 1);
                            log.add(newEntry);
                        }

                    } else {
                        log.add(newEntry);
                    }
                    insertPos++;
                }
                if (leaderCommit > commitIndex) {
                    commitIndex = Math.min(leaderCommit, log.size() - 1);
                }
                success = true;
            }

            if (currentTerm != termBefore || !entries.isEmpty()) {
                latchToAwait = requestPersist();
            }
            responseTerm = currentTerm;
        }

        if (latchToAwait != null) awaitPersist(latchToAwait);
        return responseTerm + " " + success;
    }

    private String handleSubmit(String command) {
        long myIndex;
        CountDownLatch persistLatch;
        CountDownLatch commitLatch;

        synchronized (stateLock) {
            if (state != State.LEADER) {
                return "NOT_LEADER " + (leaderId != null ? leaderId : -1);
            }
            myIndex = log.size();
            log.add(new LogEntry(currentTerm, myIndex, command));
            persistLatch = requestPersist();
            commitLatch = new CountDownLatch(1);
            pendingSubmissions.put(myIndex, commitLatch);
        }

        awaitPersist(persistLatch);
        controlPool.submit(this::replicateToAllPeers);

        try {
            boolean committed = commitLatch.await(SUBMIT_COMMIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            return committed ? ("OK " + myIndex) : ("TIMEOUT " + myIndex);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "ERR interrupted";
        }
    }


    private void stepDownToFollower(long newTerm) {
        currentTerm = newTerm;
        votedFor = null;
        state = State.FOLLOWER;
    }

    // ================= BATCHED PERSISTENCE =================


    private CountDownLatch requestPersist() {
        synchronized (persistLock) {
            persistDirty = true;
            return currentPersistBatch;
        }
    }

    private void awaitPersist(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void persistFlusherLoop() {
        while (running) {
            sleep(PERSIST_BATCH_INTERVAL_MS);
            flushPersistBatch();
        }
        flushPersistBatch();
    }

    private void flushPersistBatch() {
        CountDownLatch latchToRelease;
        synchronized (persistLock) {
            if (!persistDirty) return;
            latchToRelease = currentPersistBatch;
            persistDirty = false;
            currentPersistBatch = new CountDownLatch(1);
        }

        long termSnapshot;
        Integer votedForSnapshot;
        long currentLogSize;
        List<LogEntry> newEntries = null;
        List<LogEntry> fullLogForRewrite = null;

        synchronized (stateLock) {
            termSnapshot = currentTerm;
            votedForSnapshot = votedFor;
            currentLogSize = log.size();
            if (currentLogSize > persistedLogSize) {

                newEntries = new ArrayList<>(log.subList((int) persistedLogSize, (int) currentLogSize));
            } else if (currentLogSize < persistedLogSize) {

                fullLogForRewrite = new ArrayList<>(log);
            }
        }

        try {

            File headerTmp = new File(headerFile.getPath() + ".tmp");
            try (FileOutputStream fos = new FileOutputStream(headerTmp)) {
                String content = termSnapshot + "\n" + (votedForSnapshot == null ? -1 : votedForSnapshot) + "\n";
                fos.write(content.getBytes(StandardCharsets.UTF_8));
                fos.flush();
                fos.getFD().sync();
            }
            Files.move(headerTmp.toPath(), headerFile.toPath(),
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

            if (fullLogForRewrite != null) {
                rewriteEntireLogFile(fullLogForRewrite);
                persistedLogSize = fullLogForRewrite.size();
            } else if (newEntries != null && !newEntries.isEmpty()) {

                appendNewLogEntries(newEntries);
                persistedLogSize = currentLogSize;
            }
        } catch (IOException e) {
            log("WARNING: failed to persist Raft state: " + e.getMessage());
        } finally {
            latchToRelease.countDown();
        }
    }

    private void appendNewLogEntries(List<LogEntry> newEntries) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (LogEntry e : newEntries) {
            sb.append(e.term).append(' ').append(e.index).append('\n');
            sb.append(e.command).append('\n');
        }
        try (FileOutputStream fos = new FileOutputStream(logFile, true)) { // true = append mode
            fos.write(sb.toString().getBytes(StandardCharsets.UTF_8));
            fos.flush();
            fos.getFD().sync();
        }
    }

    private void rewriteEntireLogFile(List<LogEntry> fullLog) throws IOException {
        File tmp = new File(logFile.getPath() + ".tmp");
        try (FileOutputStream fos = new FileOutputStream(tmp)) {
            StringBuilder sb = new StringBuilder();
            for (LogEntry e : fullLog) {
                sb.append(e.term).append(' ').append(e.index).append('\n');
                sb.append(e.command).append('\n');
            }
            fos.write(sb.toString().getBytes(StandardCharsets.UTF_8));
            fos.flush();
            fos.getFD().sync();
        }
        Files.move(tmp.toPath(), logFile.toPath(),
                StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }


    private void loadPersistedState() {
        boolean headerExists = headerFile.exists() && headerFile.length() > 0;

        if (!headerExists) {
            log.add(new LogEntry(0, 0, ""));
            persistedLogSize = log.size();
            return;
        }

        try (BufferedReader headerReader = new BufferedReader(
                new InputStreamReader(new FileInputStream(headerFile), StandardCharsets.UTF_8))) {
            currentTerm = Long.parseLong(headerReader.readLine());
            long votedForRaw = Long.parseLong(headerReader.readLine());
            votedFor = (votedForRaw == -1) ? null : (int) votedForRaw;
        } catch (IOException | NumberFormatException | NullPointerException e) {
            log("WARNING: header file unreadable/corrupt (" + e.getMessage() + "), starting fresh");
            currentTerm = 0;
            votedFor = null;
            log.clear();
            log.add(new LogEntry(0, 0, ""));
            persistedLogSize = log.size();
            return;
        }

        List<LogEntry> loaded = new ArrayList<>();
        if (logFile.exists()) {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(new FileInputStream(logFile), StandardCharsets.UTF_8))) {
                String meta;
                while ((meta = reader.readLine()) != null) {
                    String command = reader.readLine();
                    if (command == null) break;
                    String[] parts = meta.trim().split("\\s+");
                    loaded.add(new LogEntry(Long.parseLong(parts[0]), Long.parseLong(parts[1]), command));
                }
            } catch (IOException | NumberFormatException e) {
                log("WARNING: log file unreadable/corrupt (" + e.getMessage() + "), starting with empty log");
                loaded.clear();
            }
        }

        if (loaded.isEmpty()) {
            loaded.add(new LogEntry(0, 0, ""));
        }

        log.clear();
        log.addAll(loaded);
        persistedLogSize = log.size();
        log("Recovered persisted state: term=" + currentTerm + " votedFor=" + votedFor + " logSize=" + log.size());
    }

    // ================= ELECTION TIMER =================

    private void electionTimerLoop() {
        while (running) {
            sleep(ELECTION_POLL_INTERVAL_MS);
            ElectionStart es = null;
            synchronized (stateLock) {
                if (state == State.LEADER) continue;
                long elapsed = System.currentTimeMillis() - lastContactTime;
                if (elapsed > currentTimeoutMs) {
                    es = beginElection();
                }
            }
            if (es != null) {
                runElection(es);
            }
        }
    }

    private void resetElectionTimer() {
        lastContactTime = System.currentTimeMillis();
        currentTimeoutMs = randomElectionTimeout();
    }

    private long randomElectionTimeout() {
        return ELECTION_TIMEOUT_MIN_MS + (long) (Math.random() * (ELECTION_TIMEOUT_MAX_MS - ELECTION_TIMEOUT_MIN_MS));
    }

    // ================= ELECTION (candidate side) =================

    private static class ElectionStart {
        final long myTerm;
        final long myLastLogIndex;
        final long myLastLogTerm;
        final CountDownLatch persistLatch;
        ElectionStart(long myTerm, long myLastLogIndex, long myLastLogTerm, CountDownLatch persistLatch) {
            this.myTerm = myTerm;
            this.myLastLogIndex = myLastLogIndex;
            this.myLastLogTerm = myLastLogTerm;
            this.persistLatch = persistLatch;
        }
    }


    private ElectionStart beginElection() {
        state = State.CANDIDATE;
        currentTerm++;
        votedFor = nodeId;
        resetElectionTimer();
        CountDownLatch latch = requestPersist();
        log("Starting election for term " + currentTerm);
        LogEntry myLast = log.get(log.size() - 1);
        return new ElectionStart(currentTerm, myLast.index, myLast.term, latch);
    }


    private void runElection(ElectionStart es) {
        awaitPersist(es.persistLatch);

        AtomicInteger votesReceived = new AtomicInteger(1);
        int majority = (peers.size() + 1) / 2 + 1;

        for (String peer : peers) {
            controlPool.submit(() -> {
                RpcResult result = sendRequestVote(peer, es.myTerm, nodeId, es.myLastLogIndex, es.myLastLogTerm);
                if (result == null) return;

                CountDownLatch[] leaderPersistLatch = new CountDownLatch[1];
                synchronized (stateLock) {
                    if (result.term > currentTerm) {
                        stepDownToFollower(result.term);

                        requestPersist();
                        return;
                    }
                    if (state == State.CANDIDATE && currentTerm == es.myTerm && result.success) {
                        int votes = votesReceived.incrementAndGet();
                        if (votes >= majority) {
                            leaderPersistLatch[0] = tryBecomeLeaderMutateOnly(es.myTerm);
                        }
                    }
                }

                if (leaderPersistLatch[0] != null) {
                    awaitPersist(leaderPersistLatch[0]);
                    controlPool.submit(this::replicateToAllPeers);
                }
            });
        }
    }


    private CountDownLatch tryBecomeLeaderMutateOnly(long term) {
        if (state != State.CANDIDATE || currentTerm != term) return null;
        state = State.LEADER;
        leaderId = nodeId;
        log(">>> Became LEADER for term " + term + " <<<");

        nextIndex.clear();
        matchIndex.clear();
        for (String peer : peers) {
            nextIndex.put(peer, (long) log.size());
            matchIndex.put(peer, 0L);
        }


        long noopIndex = log.size();
        log.add(new LogEntry(term, noopIndex, "NOOP"));
        return requestPersist();
    }

    // ================= REPLICATION (leader side) =================

    private void heartbeatLoop() {
        while (running) {
            boolean isLeader;
            synchronized (stateLock) {
                isLeader = (state == State.LEADER);
            }
            if (isLeader) {
                replicateToAllPeers();
            }
            sleep(HEARTBEAT_INTERVAL_MS);
        }
    }

    private void replicateToAllPeers() {
        long term;
        synchronized (stateLock) {
            if (state != State.LEADER) return;
            term = currentTerm;
        }
        for (String peer : peers) {

            if (peerInFlight.putIfAbsent(peer, Boolean.TRUE) != null) {
                continue;
            }
            controlPool.submit(() -> {
                try {
                    replicateToPeer(peer, term);
                } finally {
                    peerInFlight.remove(peer);
                }
            });
        }
    }

    private void replicateToPeer(String peer, long term) {
        long prevLogIndex;
        long prevLogTerm;
        List<LogEntry> entriesToSend;

        synchronized (stateLock) {
            if (state != State.LEADER || currentTerm != term) return;
            long nextIdx = nextIndex.getOrDefault(peer, (long) log.size());
            prevLogIndex = nextIdx - 1;
            prevLogTerm = log.get((int) prevLogIndex).term;
            entriesToSend = new ArrayList<>(log.subList((int) nextIdx, log.size()));
        }

        RpcResult result = sendAppendEntries(peer, term, nodeId, prevLogIndex, prevLogTerm, getCommitIndexSnapshot(), entriesToSend);
        if (result == null) return;

        synchronized (stateLock) {
            if (result.term > currentTerm) {
                stepDownToFollower(result.term);
                requestPersist();
                log("Stepped down: discovered higher term " + result.term + " from peer " + peer);
                return;
            }
            if (state != State.LEADER || currentTerm != term) return;

            if (result.success) {
                long newMatchIndex = prevLogIndex + entriesToSend.size();
                matchIndex.put(peer, newMatchIndex);
                nextIndex.put(peer, newMatchIndex + 1);
                tryAdvanceCommitIndex();
            } else {
                long current = nextIndex.getOrDefault(peer, 1L);
                nextIndex.put(peer, Math.max(1, current - 1));
            }
        }
    }


    private void tryAdvanceCommitIndex() {
        int majority = (peers.size() + 1) / 2 + 1;
        for (long n = log.size() - 1; n > commitIndex; n--) {
            if (log.get((int) n).term != currentTerm) continue;

            int count = 1;
            for (String peer : peers) {
                if (matchIndex.getOrDefault(peer, 0L) >= n) count++;
            }
            if (count >= majority) {
                commitIndex = n;
                log("Advanced commitIndex to " + n);
                pendingSubmissions.entrySet().removeIf(e -> {
                    if (e.getKey() <= commitIndex) {
                        e.getValue().countDown();
                        return true;
                    }
                    return false;
                });
                break;
            }
        }
    }

    private long getCommitIndexSnapshot() {
        synchronized (stateLock) {
            return commitIndex;
        }
    }


    private void applyCommandToStore(String command) {
        List<String> parts = tokenize(command);
        if (parts.isEmpty()) return;
        String cmd = parts.get(0).toUpperCase();

        switch (cmd) {
            case "SET":
                store.set(parts.get(1), parts.get(2));
                break;
            case "SETEX":
                store.setWithTtl(parts.get(1), parts.get(3), Long.parseLong(parts.get(2)));
                break;
            case "DEL":
                store.delete(parts.get(1));
                break;
            default:
                log("WARNING: unrecognized committed command, ignoring: " + command);
        }
    }

    private List<String> tokenize(String line) {
        List<String> tokens = new ArrayList<>();
        Matcher m = TOKEN_PATTERN.matcher(line);
        while (m.find()) {
            tokens.add(m.group(1) != null ? m.group(1) : m.group(2));
        }
        return tokens;
    }

    // ================= CLIENT-FACING KV SERVER =================

    private void runClientServer() {
        try (ServerSocket serverSocket = new ServerSocket(clientPort)) {
            log("Client-facing KV server listening on port " + clientPort);
            while (running) {
                Socket socket = serverSocket.accept();
                clientPool.submit(() -> handleClientConnection(socket));
            }
        } catch (IOException e) {
            log("Client server error: " + e.getMessage());
        }
    }

    private void handleClientConnection(Socket socket) {
        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                OutputStream out = socket.getOutputStream()
        ) {
            String line;
            while ((line = in.readLine()) != null) {
                String response = handleClientCommand(line.trim());
                out.write(response.getBytes());
                out.flush();
            }
        } catch (IOException e) {

        } finally {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    private String handleClientCommand(String line) {
        if (line.isEmpty()) return "-ERR empty command\r\n";
        List<String> parts = tokenize(line);
        String cmd = parts.get(0).toUpperCase();

        switch (cmd) {
            case "GET": {

                if (parts.size() < 2) return "-ERR GET requires a key\r\n";
                String val = store.get(parts.get(1));
                return val == null ? "$-1\r\n" : "$" + val.length() + "\r\n" + val + "\r\n";
            }
            case "TTL": {
                if (parts.size() < 2) return "-ERR TTL requires a key\r\n";
                return ":" + store.ttl(parts.get(1)) + "\r\n";
            }
            case "SET": {
                if (parts.size() < 3) return "-ERR SET requires key and value\r\n";
                String raftCommand = "SET " + parts.get(1) + " \"" + parts.get(2) + "\"";
                return submitAndTranslate(raftCommand);
            }
            case "SETEX": {
                if (parts.size() < 4) return "-ERR SETEX requires key, ttl, and value\r\n";
                String raftCommand = "SETEX " + parts.get(1) + " " + parts.get(2) + " \"" + parts.get(3) + "\"";
                return submitAndTranslate(raftCommand);
            }
            case "DEL": {
                if (parts.size() < 2) return "-ERR DEL requires a key\r\n";

                boolean existedBefore = store.get(parts.get(1)) != null;
                String result = handleSubmit("DEL " + parts.get(1));
                if (result.startsWith("OK")) return ":" + (existedBefore ? 1 : 0) + "\r\n";
                return translateSubmitError(result);
            }
            case "STATUS": {
                synchronized (stateLock) {
                    String s = "state=" + state + " term=" + currentTerm + " commitIndex=" + commitIndex
                            + " logSize=" + log.size() + " leaderId=" + leaderId + " storeSize=" + store.size();
                    return "$" + s.length() + "\r\n" + s + "\r\n";
                }
            }
            default:
                return "-ERR unknown command '" + cmd + "'\r\n";
        }
    }

    private String submitAndTranslate(String raftCommand) {
        String result = handleSubmit(raftCommand);
        if (result.startsWith("OK")) return "+OK\r\n";
        return translateSubmitError(result);
    }

    private String translateSubmitError(String result) {
        if (result.startsWith("NOT_LEADER")) {
            String[] p = result.split("\\s+");
            String leaderHint = p.length > 1 ? p[1] : "unknown";
            return "-ERR not leader, try node " + leaderHint + "\r\n";
        }
        if (result.startsWith("TIMEOUT")) return "-ERR commit timeout, try again\r\n";
        return "-ERR " + result + "\r\n";
    }

    // ================= APPLY LOOP (both leader and followers) =================

    private void applyLoop() {
        while (running) {
            sleep(APPLY_POLL_INTERVAL_MS);
            List<LogEntry> toApply = new ArrayList<>();
            synchronized (stateLock) {
                while (lastApplied < commitIndex) {
                    lastApplied++;
                    toApply.add(log.get((int) lastApplied));
                }
            }
            for (LogEntry entry : toApply) {
                if (!entry.command.equals("NOOP")) {
                    applyCallback.accept(entry);
                }
            }
        }
    }

    // ================= RPC CLIENT (sending side) =================

    private static class RpcResult {
        final long term;
        final boolean success;
        RpcResult(long term, boolean success) {
            this.term = term;
            this.success = success;
        }
    }

    private RpcResult sendRequestVote(String peer, long term, int candidateId, long lastLogIndex, long lastLogTerm) {
        String request = "REQUEST_VOTE " + term + " " + candidateId + " " + lastLogIndex + " " + lastLogTerm;
        return sendRpc(peer, request);
    }

    private RpcResult sendAppendEntries(String peer, long term, int leaderIdParam, long prevLogIndex,
                                        long prevLogTerm, long leaderCommit, List<LogEntry> entries) {
        StringBuilder sb = new StringBuilder();
        sb.append("APPEND_ENTRIES ").append(term).append(' ').append(leaderIdParam).append(' ')
                .append(prevLogIndex).append(' ').append(prevLogTerm).append(' ')
                .append(leaderCommit).append(' ').append(entries.size()).append('\n');
        for (LogEntry e : entries) {
            sb.append(e.term).append(' ').append(e.index).append('\n');
            sb.append(e.command).append('\n');
        }
        return sendRpc(peer, sb.toString());
    }

    private RpcResult sendRpc(String peer, String request) {
        String[] hostPort = peer.split(":");
        try (Socket socket = new Socket()) {
            socket.connect(new java.net.InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1])), RPC_TIMEOUT_MS);
            socket.setSoTimeout(RPC_TIMEOUT_MS);

            OutputStream out = socket.getOutputStream();
            out.write(request.getBytes());
            if (!request.endsWith("\n")) out.write('\n');
            out.flush();

            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String line = in.readLine();
            if (line == null) return null;

            String[] parts = line.trim().split("\\s+");
            return new RpcResult(Long.parseLong(parts[0]), Boolean.parseBoolean(parts[1]));
        } catch (IOException e) {
            return null;
        }
    }

    // ================= UTIL =================

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void log(String message) {
        System.out.println("[node-" + nodeId + " term=" + currentTerm + "] " + message);
    }
}