package com.aditya.kvstore.store;

import com.aditya.kvstore.replication.ReplicationHub;
import com.aditya.kvstore.wal.WriteAheadLog;
import com.aditya.kvstore.wal.WriteAheadLog.LogEntry;
import com.aditya.kvstore.wal.WriteAheadLog.OpType;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;


public class KvEngine {

    private final KeyValueStore store;
    private final WriteAheadLog wal;
    private final ReplicationHub hub = new ReplicationHub();



    private final Object replicationLock = new Object();

    public KvEngine(String walPath, int maxEntries, long batchIntervalMs) {
        this.wal = new WriteAheadLog(walPath, batchIntervalMs);
        this.store = new KeyValueStore(maxEntries);
    }


    public void start() throws IOException {
        wal.open();
        recover();
    }

    private void recover() throws IOException {
        System.out.println("Replaying WAL for crash recovery...");
        final int[] count = {0};
        wal.replay(entry -> {
            if (entry.op == OpType.SET) {
                store.setAbsolute(entry.key, entry.value, entry.expiresAt);
            } else if (entry.op == OpType.DEL) {
                store.delete(entry.key);
            }
            count[0]++;
        });
        System.out.println("Recovery complete. Replayed " + count[0]
                + " log entries. Current store size = " + store.size());
    }

    public void set(String key, String value) throws IOException {

        wal.append(new LogEntry(OpType.SET, key, value, -1));
        synchronized (replicationLock) {
            store.set(key, value);
            hub.broadcast("SET " + key + " \"" + value + "\"");
        }
    }

    public void setWithTtl(String key, String value, long ttlSeconds) throws IOException {
        long expiresAt = System.currentTimeMillis() + ttlSeconds * 1000;
        wal.append(new LogEntry(OpType.SET, key, value, expiresAt));
        synchronized (replicationLock) {
            store.setAbsolute(key, value, expiresAt);

            hub.broadcast("SETEX " + key + " " + ttlSeconds + " \"" + value + "\"");
        }
    }

    public String get(String key) {

        return store.get(key);
    }

    public boolean delete(String key) throws IOException {
        wal.append(new LogEntry(OpType.DEL, key, null, -1));
        synchronized (replicationLock) {
            boolean removed = store.delete(key);
            hub.broadcast("DEL " + key);
            return removed;
        }
    }

    public long ttl(String key) {
        return store.ttl(key);
    }

    public int size() {
        return store.size();
    }

    public long evictionCount() {
        return store.getEvictionCount();
    }

    public int maxEntries() {
        return store.getMaxEntries();
    }


    public void registerReplica(OutputStream out) throws IOException {
        synchronized (replicationLock) {
            List<String> snapshot = store.exportAsCommands();
            for (String cmd : snapshot) {
                out.write((cmd + "\n").getBytes(StandardCharsets.UTF_8));
            }
            out.write("SYNCDONE\n".getBytes(StandardCharsets.UTF_8));
            out.flush();
            hub.addReplica(out);
        }
        System.out.println("Replica registered. Total replicas: " + hub.replicaCount());
    }

    public void unregisterReplica(OutputStream out) {
        hub.removeReplica(out);
        System.out.println("Replica disconnected. Total replicas: " + hub.replicaCount());
    }

    public int replicaCount() {
        return hub.replicaCount();
    }
}