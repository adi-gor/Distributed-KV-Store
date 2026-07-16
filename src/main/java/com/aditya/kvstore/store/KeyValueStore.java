package com.aditya.kvstore.store;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class KeyValueStore {


    private static class StoredValue {
        final String value;
        final long expiresAt; // -1 = never expires

        StoredValue(String value, long expiresAt) {
            this.value = value;
            this.expiresAt = expiresAt;
        }

        boolean isExpired() {
            return expiresAt != -1 && System.currentTimeMillis() > expiresAt;
        }
    }

    private final int maxEntries;
    private volatile long evictionCount = 0;


    private final LinkedHashMap<String, StoredValue> data;

    private final ScheduledExecutorService expiryScheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "kv-expiry-sweeper");
                t.setDaemon(true);
                return t;
            });

    public KeyValueStore(int maxEntries) {
        this.maxEntries = maxEntries;

        this.data = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, StoredValue> eldest) {
                boolean shouldEvict = size() > KeyValueStore.this.maxEntries;
                if (shouldEvict) {
                    evictionCount++;
                }
                return shouldEvict;
            }
        };


        expiryScheduler.scheduleAtFixedRate(this::sweepExpiredKeys, 1, 1, TimeUnit.SECONDS);
    }

    public synchronized void set(String key, String value) {
        data.put(key, new StoredValue(value, -1));
    }


    public synchronized void setWithTtl(String key, String value, long ttlSeconds) {
        long expiresAt = System.currentTimeMillis() + (ttlSeconds * 1000);
        setAbsolute(key, value, expiresAt);
    }


    public synchronized void setAbsolute(String key, String value, long expiresAt) {
        data.put(key, new StoredValue(value, expiresAt));
    }

    public synchronized String get(String key) {

        StoredValue entry = data.get(key);
        if (entry == null) return null;


        if (entry.isExpired()) {
            data.remove(key);
            return null;
        }
        return entry.value;
    }

    public synchronized boolean delete(String key) {
        return data.remove(key) != null;
    }


    public synchronized long ttl(String key) {
        StoredValue entry = data.get(key);
        if (entry == null) return -2;
        if (entry.expiresAt == -1) return -1;
        long remainingMs = entry.expiresAt - System.currentTimeMillis();
        return Math.max(remainingMs / 1000, 0);
    }

    public synchronized int size() {
        return data.size();
    }

    public long getEvictionCount() {
        return evictionCount;
    }

    public int getMaxEntries() {
        return maxEntries;
    }

    private synchronized void sweepExpiredKeys() {
        data.entrySet().removeIf(e -> e.getValue().isExpired());
    }


    public synchronized List<String> exportAsCommands() {
        List<String> commands = new ArrayList<>();
        for (Map.Entry<String, StoredValue> e : data.entrySet()) {
            StoredValue sv = e.getValue();
            if (sv.isExpired()) continue;
            if (sv.expiresAt == -1) {
                commands.add("SET " + e.getKey() + " \"" + sv.value + "\"");
            } else {
                long remainingSeconds = Math.max((sv.expiresAt - System.currentTimeMillis()) / 1000, 1);
                commands.add("SETEX " + e.getKey() + " " + remainingSeconds + " \"" + sv.value + "\"");
            }
        }
        return commands;
    }
}