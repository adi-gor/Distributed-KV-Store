package com.aditya.kvstore.raft;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;


public class RaftBenchmarkClient {

    private static final List<String> CLIENT_NODES = Arrays.asList(
            "localhost:8001", "localhost:8002", "localhost:8003");

    private static final int NUM_THREADS = 50;
    private static final int OPS_PER_THREAD = 2000;
    private static final int WARMUP_OPS_PER_THREAD = 200;
    private static final int KEYSPACE_SIZE = 1000;

    private static final AtomicReference<String> currentLeader = new AtomicReference<>();
    private static final Object leaderDiscoveryLock = new Object();
    private static final AtomicLong totalReconnects = new AtomicLong(0);

    public static void main(String[] args) throws Exception {
        System.out.println("Discovering current Raft leader...");
        String leaderNode = discoverLeader();
        currentLeader.set(leaderNode);
        System.out.println("Leader found at " + leaderNode + ". Benchmarking.\n");

        System.out.println("Threads: " + NUM_THREADS + ", ops/thread: " + OPS_PER_THREAD
                + ", keyspace: " + KEYSPACE_SIZE);

        runBenchmark("SET (via Raft consensus)", true);
        runBenchmark("GET (leader only)", false);
        runDistributedReadBenchmark();
    }

    // ================= LEADER DISCOVERY =================

    private static String discoverLeader() throws InterruptedException {
        for (int attempt = 0; attempt < 20; attempt++) {
            for (String node : CLIENT_NODES) {
                String response = sendOneShot(node, "STATUS");
                if (response == null) continue;
                Integer leaderId = parseLeaderId(response);
                if (leaderId != null && leaderId >= 0) {
                    return "localhost:" + (8001 + leaderId);
                }
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException("No leader found after retrying -- is the Raft cluster running?");
    }

    private static Integer parseLeaderId(String statusResponse) {
        int idx = statusResponse.indexOf("leaderId=");
        if (idx == -1) return null;
        String rest = statusResponse.substring(idx + "leaderId=".length());
        String token = rest.split("\\s+")[0];
        try {
            return Integer.parseInt(token);
        } catch (NumberFormatException e) {
            return null;
        }
    }


    private static String rediscoverLeaderShared() {
        synchronized (leaderDiscoveryLock) {
            try {
                String fresh = discoverLeader();
                currentLeader.set(fresh);
                totalReconnects.incrementAndGet();
                return fresh;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return currentLeader.get();
            }
        }
    }

    // ================= CONNECTION HELPER =================

    private static class Connection {
        final Socket socket;
        final BufferedReader in;
        final OutputStream out;
        Connection(Socket socket, BufferedReader in, OutputStream out) {
            this.socket = socket;
            this.in = in;
            this.out = out;
        }
        void close() {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    private static Connection connect(String node) throws IOException {
        String[] hostPort = node.split(":");
        Socket socket = new Socket(hostPort[0], Integer.parseInt(hostPort[1]));
        return new Connection(socket,
                new BufferedReader(new InputStreamReader(socket.getInputStream())),
                socket.getOutputStream());
    }

    private static String trySend(Connection conn, String cmd) {
        if (conn == null) return null;
        try {
            conn.out.write((cmd + "\n").getBytes());
            conn.out.flush();
            String first = conn.in.readLine();
            if (first == null) return null;
            if (first.startsWith("$") && !first.equals("$-1")) {
                conn.in.readLine(); // drain the value line
            }
            return first;
        } catch (IOException e) {
            return null;
        }
    }

    // ================= PRIMARY BENCHMARK =================

    private static void runBenchmark(String opType, boolean isWrite) throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(NUM_THREADS);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(NUM_THREADS);

        List<long[]> perThreadLatencies = new ArrayList<>();
        for (int i = 0; i < NUM_THREADS; i++) perThreadLatencies.add(new long[OPS_PER_THREAD]);

        AtomicLong errorCount = new AtomicLong(0);
        AtomicLong connectionFailures = new AtomicLong(0);

        for (int t = 0; t < NUM_THREADS; t++) {
            final int threadId = t;
            pool.submit(() -> {
                Connection conn = null;
                try {
                    conn = connect(currentLeader.get());
                    Random rand = new Random();

                    for (int i = 0; i < WARMUP_OPS_PER_THREAD; i++) {
                        String key = "key" + rand.nextInt(KEYSPACE_SIZE);
                        String cmd = isWrite ? "SET " + key + " val" : "GET " + key;
                        String resp = trySend(conn, cmd);
                        if (resp == null || resp.startsWith("-ERR not leader")) {
                            conn = reconnectToNewLeader(conn);
                            trySend(conn, cmd);
                        }
                    }

                    startGate.await();

                    long[] latencies = perThreadLatencies.get(threadId);
                    for (int i = 0; i < OPS_PER_THREAD; i++) {
                        String key = "key" + rand.nextInt(KEYSPACE_SIZE);
                        String cmd = isWrite ? "SET " + key + " val" : "GET " + key;

                        long start = System.nanoTime();
                        String response = trySend(conn, cmd);

                        if (response == null || response.startsWith("-ERR not leader")) {

                            conn = reconnectToNewLeader(conn);
                            response = trySend(conn, cmd);
                        }


                        long elapsed = System.nanoTime() - start;

                        if (response != null && !response.startsWith("-ERR")) {
                            latencies[i] = elapsed;
                        } else {
                            latencies[i] = -1;
                            errorCount.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    connectionFailures.incrementAndGet();
                } finally {
                    if (conn != null) conn.close();
                    doneLatch.countDown();
                }
            });
        }

        long wallStart = System.nanoTime();
        startGate.countDown();
        doneLatch.await();
        long wallElapsedNanos = System.nanoTime() - wallStart;

        pool.shutdown();
        pool.awaitTermination(30, TimeUnit.SECONDS);

        printResults(opType, perThreadLatencies, wallElapsedNanos, errorCount.get(), connectionFailures.get());
    }

    private static Connection reconnectToNewLeader(Connection oldConn) {
        if (oldConn != null) oldConn.close();
        String newLeader = rediscoverLeaderShared();
        try {
            return connect(newLeader);
        } catch (IOException e) {
            return null;
        }
    }

    // ================= distributed reads =================

    private static void runDistributedReadBenchmark() throws InterruptedException {
        System.out.println("\n=== GET distributed across all 3 nodes (read scaling) ===");

        ExecutorService pool = Executors.newFixedThreadPool(NUM_THREADS);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(NUM_THREADS);

        List<long[]> perThreadLatencies = new ArrayList<>();
        for (int i = 0; i < NUM_THREADS; i++) perThreadLatencies.add(new long[OPS_PER_THREAD]);
        AtomicLong errorCount = new AtomicLong(0);
        AtomicLong connectionFailures = new AtomicLong(0);

        for (int t = 0; t < NUM_THREADS; t++) {
            final int threadId = t;
            String node = CLIENT_NODES.get(threadId % CLIENT_NODES.size());

            pool.submit(() -> {
                Connection conn = null;
                try {
                    conn = connect(node);
                    Random rand = new Random();
                    for (int i = 0; i < WARMUP_OPS_PER_THREAD; i++) {
                        trySend(conn, "GET key" + rand.nextInt(KEYSPACE_SIZE));
                    }

                    startGate.await();

                    long[] latencies = perThreadLatencies.get(threadId);
                    for (int i = 0; i < OPS_PER_THREAD; i++) {
                        String cmd = "GET key" + rand.nextInt(KEYSPACE_SIZE);
                        long start = System.nanoTime();
                        String response = trySend(conn, cmd);
                        long elapsed = System.nanoTime() - start;
                        if (response != null && !response.startsWith("-ERR")) {
                            latencies[i] = elapsed;
                        } else {
                            latencies[i] = -1;
                            errorCount.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    connectionFailures.incrementAndGet();
                } finally {
                    if (conn != null) conn.close();
                    doneLatch.countDown();
                }
            });
        }

        long wallStart = System.nanoTime();
        startGate.countDown();
        doneLatch.await();
        long wallElapsedNanos = System.nanoTime() - wallStart;

        pool.shutdown();
        pool.awaitTermination(30, TimeUnit.SECONDS);

        printResults("GET (spread across 3 nodes)", perThreadLatencies, wallElapsedNanos, errorCount.get(), connectionFailures.get());
    }

    // ================= ONE-SHOT HELPER =================

    private static String sendOneShot(String node, String command) {
        String[] hostPort = node.split(":");
        try (Socket socket = new Socket()) {
            socket.connect(new java.net.InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1])), 300);
            socket.setSoTimeout(1000);
            OutputStream out = socket.getOutputStream();
            out.write((command + "\n").getBytes());
            out.flush();
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String first = in.readLine();
            if (first == null) return null;
            if (first.startsWith("$") && !first.equals("$-1")) {
                String second = in.readLine();
                return first + " " + second;
            }
            return first;
        } catch (IOException e) {
            return null;
        }
    }

    // ================= RESULTS =================

    private static void printResults(String opType, List<long[]> perThreadLatencies,
                                     long wallElapsedNanos, long errors, long connectionFailures) {
        List<Long> successful = new ArrayList<>();
        for (long[] arr : perThreadLatencies) {
            for (long v : arr) {
                if (v >= 0) successful.add(v);
            }
        }
        long[] all = successful.stream().mapToLong(Long::longValue).sorted().toArray();

        int totalAttempted = perThreadLatencies.size() * perThreadLatencies.get(0).length;
        double wallSeconds = wallElapsedNanos / 1_000_000_000.0;
        double throughput = all.length / wallSeconds;

        System.out.println("\n--- " + opType + " results ---");
        System.out.printf("Total attempted:     %d%n", totalAttempted);
        System.out.printf("Successful:          %d%n", all.length);
        System.out.printf("Errors (-ERR):       %d%n", errors);
        System.out.printf("Connection failures: %d%n", connectionFailures);
        System.out.printf("Leader reconnects:   %d%n", totalReconnects.get());
        System.out.printf("Wall time:           %.3f s%n", wallSeconds);
        System.out.printf("Throughput:          %.0f ops/sec%n", throughput);
        if (all.length > 0) {
            System.out.printf("Latency p50:         %.3f ms%n", nanosToMs(percentile(all, 50)));
            System.out.printf("Latency p95:         %.3f ms%n", nanosToMs(percentile(all, 95)));
            System.out.printf("Latency p99:         %.3f ms%n", nanosToMs(percentile(all, 99)));
            System.out.printf("Latency max:         %.3f ms%n", nanosToMs(all[all.length - 1]));
        } else {
            System.out.println("No successful operations -- check cluster health.");
        }
    }

    private static long percentile(long[] sortedLatencies, int p) {
        int index = (int) Math.ceil(p / 100.0 * sortedLatencies.length) - 1;
        index = Math.max(0, Math.min(index, sortedLatencies.length - 1));
        return sortedLatencies[index];
    }

    private static double nanosToMs(long nanos) {
        return nanos / 1_000_000.0;
    }
}