package com.aditya.kvstore.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


public class BenchmarkClient {

    private static final String HOST = "localhost";
    private static final int PORT = 6380;

    private static final int NUM_THREADS = 50;        // concurrent simulated clients
    private static final int OPS_PER_THREAD = 2000;    // measured ops per thread
    private static final int WARMUP_OPS_PER_THREAD = 200; // untimed warm-up ops per thread
    private static final int KEYSPACE_SIZE = 1000;     // number of distinct keys used

    public static void main(String[] args) throws Exception {
        System.out.println("Benchmarking KV store at " + HOST + ":" + PORT);
        System.out.println("Threads: " + NUM_THREADS + ", ops/thread: " + OPS_PER_THREAD
                + ", keyspace: " + KEYSPACE_SIZE);

        runBenchmark("SET", true);
        runBenchmark("GET", false);
    }

    private static void runBenchmark(String opType, boolean isWrite) throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(NUM_THREADS);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(NUM_THREADS);


        List<long[]> perThreadLatencies = new ArrayList<>();
        for (int i = 0; i < NUM_THREADS; i++) perThreadLatencies.add(new long[OPS_PER_THREAD]);

        AtomicLong errorCount = new AtomicLong(0);

        for (int t = 0; t < NUM_THREADS; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try (
                        Socket socket = new Socket(HOST, PORT);
                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        OutputStream out = socket.getOutputStream()
                ) {
                    java.util.Random rand = new java.util.Random();


                    for (int i = 0; i < WARMUP_OPS_PER_THREAD; i++) {
                        String key = "key" + rand.nextInt(KEYSPACE_SIZE);
                        sendCommand(out, in, isWrite ? "SET " + key + " val" : "GET " + key);
                    }

                    startGate.await();

                    long[] latencies = perThreadLatencies.get(threadId);
                    for (int i = 0; i < OPS_PER_THREAD; i++) {
                        String key = "key" + rand.nextInt(KEYSPACE_SIZE);
                        String cmd = isWrite ? "SET " + key + " val" : "GET " + key;

                        long start = System.nanoTime();
                        sendCommand(out, in, cmd);
                        long elapsed = System.nanoTime() - start;

                        latencies[i] = elapsed;
                    }
                } catch (IOException | InterruptedException e) {
                    errorCount.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        long wallStart = System.nanoTime();
        startGate.countDown();
        doneLatch.await();
        long wallElapsedNanos = System.nanoTime() - wallStart;

        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);

        printResults(opType, perThreadLatencies, wallElapsedNanos, errorCount.get());
    }

    private static void sendCommand(OutputStream out, BufferedReader in, String cmd) throws IOException {
        out.write((cmd + "\n").getBytes());
        out.flush();

        String responseLine = in.readLine();

        if (responseLine != null && responseLine.startsWith("$") && !responseLine.equals("$-1")) {
            in.readLine();
        }
    }

    private static void printResults(String opType, List<long[]> perThreadLatencies,
                                     long wallElapsedNanos, long errors) {
        int totalOps = perThreadLatencies.size() * perThreadLatencies.get(0).length;


        long[] all = new long[totalOps];
        int idx = 0;
        for (long[] arr : perThreadLatencies) {
            for (long v : arr) all[idx++] = v;
        }
        Arrays.sort(all);

        double wallSeconds = wallElapsedNanos / 1_000_000_000.0;
        double throughput = totalOps / wallSeconds;

        System.out.println("\n--- " + opType + " results ---");
        System.out.printf("Total ops:     %d%n", totalOps);
        System.out.printf("Errors:        %d%n", errors);
        System.out.printf("Wall time:     %.3f s%n", wallSeconds);
        System.out.printf("Throughput:    %.0f ops/sec%n", throughput);
        System.out.printf("Latency p50:   %.3f ms%n", nanosToMs(percentile(all, 50)));
        System.out.printf("Latency p95:   %.3f ms%n", nanosToMs(percentile(all, 95)));
        System.out.printf("Latency p99:   %.3f ms%n", nanosToMs(percentile(all, 99)));
        System.out.printf("Latency max:   %.3f ms%n", nanosToMs(all[all.length - 1]));
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