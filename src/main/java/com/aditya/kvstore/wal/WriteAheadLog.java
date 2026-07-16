package com.aditya.kvstore.wal;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class WriteAheadLog {

    public enum OpType { SET, DEL }

    public static class LogEntry {
        public final OpType op;
        public final String key;
        public final String value;   // null for DEL
        public final long expiresAt; // -1 = no expiry; irrelevant for DEL

        public LogEntry(OpType op, String key, String value, long expiresAt) {
            this.op = op;
            this.key = key;
            this.value = value;
            this.expiresAt = expiresAt;
        }
    }

    private final File file;
    private FileOutputStream fos;

    private final long batchIntervalMs;

    private final Object batchLock = new Object();
    private List<LogEntry> pendingEntries = new ArrayList<>();
    private CountDownLatch pendingLatch = new CountDownLatch(1);
    private final AtomicReference<IOException> lastFlushError = new AtomicReference<>();

    private Thread flusherThread;
    private volatile boolean running = false;

    public WriteAheadLog(String path, long batchIntervalMs) {
        this.file = new File(path);
        this.batchIntervalMs = batchIntervalMs;
    }

    public synchronized void open() throws IOException {
        File parent = file.getParentFile();
        if (parent != null) parent.mkdirs();
        fos = new FileOutputStream(file, true);

        running = true;
        flusherThread = new Thread(this::flusherLoop, "kv-wal-flusher");
        flusherThread.setDaemon(true);
        flusherThread.start();
    }

    public void append(LogEntry entry) throws IOException {
        CountDownLatch latch;
        synchronized (batchLock) {
            pendingEntries.add(entry);
            latch = pendingLatch;
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for WAL batch commit", e);
        }

        IOException err = lastFlushError.get();
        if (err != null) {
            throw new IOException("WAL flush failed: " + err.getMessage(), err);
        }
    }

    private void flusherLoop() {
        while (running) {
            try {
                Thread.sleep(batchIntervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            flushPendingBatch();
        }

        flushPendingBatch();
    }

    private void flushPendingBatch() {
        List<LogEntry> batch;
        CountDownLatch latchToRelease;

        synchronized (batchLock) {
            if (pendingEntries.isEmpty()) return;
            batch = pendingEntries;
            latchToRelease = pendingLatch;
            pendingEntries = new ArrayList<>();
            pendingLatch = new CountDownLatch(1);
        }

        try {
            StringBuilder sb = new StringBuilder();
            for (LogEntry entry : batch) {
                encodeEntry(sb, entry);
            }
            byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);

            fos.write(bytes);
            fos.flush();
            fos.getFD().sync();

            lastFlushError.set(null);
        } catch (IOException e) {
            lastFlushError.set(e);
        } finally {

            latchToRelease.countDown();
        }
    }

    private void encodeEntry(StringBuilder sb, LogEntry entry) {
        sb.append(entry.op.name()).append('\n');
        writeLengthPrefixed(sb, entry.key);
        if (entry.op == OpType.SET) {
            writeLengthPrefixed(sb, entry.value);
            sb.append(entry.expiresAt).append('\n');
        }
        sb.append("---\n");
    }

    private void writeLengthPrefixed(StringBuilder sb, String s) {
        sb.append(s.length()).append('\n').append(s).append('\n');
    }


    public synchronized void replay(Consumer<LogEntry> applyFn) throws IOException {
        if (!file.exists()) return;

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {

            String opLine;
            while ((opLine = reader.readLine()) != null) {
                if (opLine.isBlank()) continue;
                OpType op = OpType.valueOf(opLine);
                String key = readLengthPrefixed(reader);

                if (op == OpType.SET) {
                    String value = readLengthPrefixed(reader);
                    long expiresAt = Long.parseLong(reader.readLine());
                    applyFn.accept(new LogEntry(op, key, value, expiresAt));
                } else {
                    applyFn.accept(new LogEntry(op, key, null, -1));
                }

                reader.readLine();
            }
        }
    }

    private String readLengthPrefixed(BufferedReader reader) throws IOException {
        int len = Integer.parseInt(reader.readLine());
        char[] buf = new char[len];
        int readTotal = 0;
        while (readTotal < len) {
            int r = reader.read(buf, readTotal, len - readTotal);
            if (r == -1) throw new EOFException("Unexpected end of WAL while reading a value");
            readTotal += r;
        }
        reader.readLine();
        return new String(buf);
    }

    public void close() throws IOException {
        running = false;
        if (flusherThread != null) {
            flusherThread.interrupt();
            try {
                flusherThread.join(2000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        if (fos != null) fos.close();
    }
}