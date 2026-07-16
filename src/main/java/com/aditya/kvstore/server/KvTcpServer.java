package com.aditya.kvstore.server;

import com.aditya.kvstore.store.KvEngine;
import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class KvTcpServer {

    private static final int PORT = 6380;
    private static final String WAL_PATH = "data/kvstore.wal";
    private static final int MAX_ENTRIES = 2000;


    private static final long WAL_BATCH_INTERVAL_MS = 5;

    private final KvEngine engine = new KvEngine(WAL_PATH, MAX_ENTRIES, WAL_BATCH_INTERVAL_MS);

    private final ExecutorService clientPool = Executors.newCachedThreadPool();

    @PostConstruct
    public void start() throws IOException {

        engine.start();

        Thread serverThread = new Thread(this::runServer, "kv-tcp-acceptor");
        serverThread.setDaemon(false);
        serverThread.start();
    }

    private void runServer() {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("KV store listening on port " + PORT);
            while (true) {
                Socket clientSocket = serverSocket.accept(); // blocks until a client connects
                clientPool.submit(new ClientHandler(clientSocket, engine));
            }
        } catch (IOException e) {
            throw new RuntimeException("Server failed to start", e);
        }
    }
}