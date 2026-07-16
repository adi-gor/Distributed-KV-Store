package com.aditya.kvstore.replication;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;


public class ReplicationHub {


    private final List<OutputStream> replicaStreams = new CopyOnWriteArrayList<>();

    public void addReplica(OutputStream out) {
        replicaStreams.add(out);
    }

    public void removeReplica(OutputStream out) {
        replicaStreams.remove(out);
    }


    public void broadcast(String commandLine) {
        if (replicaStreams.isEmpty()) return;

        byte[] bytes = (commandLine + "\n").getBytes(StandardCharsets.UTF_8);
        for (OutputStream out : replicaStreams) {
            try {
                out.write(bytes);
                out.flush();
            } catch (IOException e) {

                System.out.println("Dropping unresponsive replica: " + e.getMessage());
                replicaStreams.remove(out);
            }
        }
    }

    public int replicaCount() {
        return replicaStreams.size();
    }
}