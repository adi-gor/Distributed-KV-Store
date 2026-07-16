package com.aditya.kvstore.server;

import com.aditya.kvstore.store.KvEngine;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ClientHandler implements Runnable {

    private final Socket socket;
    private final KvEngine engine;


    private static final Pattern TOKEN_PATTERN = Pattern.compile("\"([^\"]*)\"|(\\S+)");

    public ClientHandler(Socket socket, KvEngine engine) {
        this.socket = socket;
        this.engine = engine;
    }

    @Override
    public void run() {
        OutputStream out = null;
        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
        ) {
            out = socket.getOutputStream();
            String line;
            while ((line = in.readLine()) != null) {
                String response = handleCommand(line.trim(), out);

                if (response != null) {
                    out.write(response.getBytes());
                    out.flush();
                }
            }
        } catch (IOException e) {
            System.out.println("Client disconnected: " + e.getMessage());
        } finally {

            if (out != null) {
                engine.unregisterReplica(out);
            }
            try { socket.close(); } catch (IOException ignored) {}
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

    private String handleCommand(String line, OutputStream out) {
        if (line.isEmpty()) return "-ERR empty command\r\n";

        List<String> parts = tokenize(line);
        String cmd = parts.get(0).toUpperCase();

        try {
            switch (cmd) {
                case "SET": {
                    if (parts.size() < 3) return "-ERR SET requires key and value\r\n";
                    engine.set(parts.get(1), parts.get(2));
                    return "+OK\r\n";
                }

                case "SETEX": {
                    // SETEX key ttlSeconds value
                    if (parts.size() < 4) return "-ERR SETEX requires key, ttl, and value\r\n";
                    long ttl;
                    try {
                        ttl = Long.parseLong(parts.get(2));
                    } catch (NumberFormatException e) {
                        return "-ERR ttl must be an integer\r\n";
                    }
                    engine.setWithTtl(parts.get(1), parts.get(3), ttl);
                    return "+OK\r\n";
                }

                case "GET": {
                    if (parts.size() < 2) return "-ERR GET requires a key\r\n";
                    String val = engine.get(parts.get(1));
                    return val == null
                            ? "$-1\r\n"
                            : "$" + val.length() + "\r\n" + val + "\r\n";
                }

                case "DEL": {
                    if (parts.size() < 2) return "-ERR DEL requires a key\r\n";
                    boolean removed = engine.delete(parts.get(1));
                    return ":" + (removed ? 1 : 0) + "\r\n";
                }

                case "TTL": {
                    if (parts.size() < 2) return "-ERR TTL requires a key\r\n";
                    long ttl = engine.ttl(parts.get(1));
                    return ":" + ttl + "\r\n";
                }

                case "STATS": {
                    String stats = "size=" + engine.size()
                            + " maxEntries=" + engine.maxEntries()
                            + " evictions=" + engine.evictionCount()
                            + " replicas=" + engine.replicaCount();
                    return "$" + stats.length() + "\r\n" + stats + "\r\n";
                }

                case "SYNC": {

                    engine.registerReplica(out);
                    return null;
                }

                default:
                    return "-ERR unknown command '" + cmd + "'\r\n";
            }
        } catch (IOException e) {

            return "-ERR write failed: " + e.getMessage() + "\r\n";
        }
    }
}