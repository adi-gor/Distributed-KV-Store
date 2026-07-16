package com.aditya.kvstore.replication;

import com.aditya.kvstore.store.KvEngine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ReplicaNode {

    private static final Pattern TOKEN_PATTERN = Pattern.compile("\"([^\"]*)\"|(\\S+)");

    public static void main(String[] args) throws Exception {
        String leaderHost = "localhost";
        int leaderPort = 6380;


        KvEngine localEngine = new KvEngine("data/replica.wal", 2000, 5);
        localEngine.start();

        System.out.println("Replica starting. Connecting to leader at "
                + leaderHost + ":" + leaderPort);

        try (
                Socket socket = new Socket(leaderHost, leaderPort);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                OutputStream out = socket.getOutputStream()
        ) {
            out.write("SYNC\n".getBytes());
            out.flush();

            String line;
            long applied = 0;
            boolean syncComplete = false;

            while ((line = in.readLine()) != null) {
                if (line.equals("SYNCDONE")) {
                    syncComplete = true;
                    System.out.println("Initial sync complete. " + applied
                            + " keys loaded. Now streaming live writes...");
                    continue;
                }

                applyCommand(localEngine, line.trim());
                applied++;

                if (syncComplete) {
                    System.out.println("Applied live write #" + applied
                            + ". Local store size = " + localEngine.size());
                } else if (applied % 1000 == 0) {
                    System.out.println("Loading snapshot... " + applied + " keys so far");
                }
            }
        }

        System.out.println("Leader connection closed. Replica stopping.");
    }

    private static void applyCommand(KvEngine engine, String line) throws IOException {
        if (line.isEmpty()) return;
        List<String> parts = tokenize(line);
        String cmd = parts.get(0).toUpperCase();

        switch (cmd) {
            case "SET":
                engine.set(parts.get(1), parts.get(2));
                break;
            case "SETEX":
                engine.setWithTtl(parts.get(1), parts.get(3), Long.parseLong(parts.get(2)));
                break;
            case "DEL":
                engine.delete(parts.get(1));
                break;
            default:
                System.out.println("Replica: ignoring unrecognized replicated command: " + cmd);
        }
    }

    private static List<String> tokenize(String line) {
        List<String> tokens = new ArrayList<>();
        Matcher m = TOKEN_PATTERN.matcher(line);
        while (m.find()) {
            tokens.add(m.group(1) != null ? m.group(1) : m.group(2));
        }
        return tokens;
    }
}