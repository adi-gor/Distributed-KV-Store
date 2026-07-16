package com.aditya.kvstore.raft;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;


public class RaftTestClient {

    public static void main(String[] args) throws Exception {
        List<String> nodes = Arrays.asList("localhost:7001", "localhost:7002", "localhost:7003");

        System.out.println("Connected to Raft cluster: " + nodes);
        System.out.println("Type a command (e.g. SET foo bar) to submit it, or STATUS to check all nodes.");
        System.out.println("Type EXIT to quit.\n");

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("> ");
            String line = scanner.nextLine();
            if (line.equalsIgnoreCase("EXIT")) break;

            if (line.equalsIgnoreCase("STATUS")) {
                for (String node : nodes) {
                    String status = sendRpc(node, "STATUS\n");
                    System.out.println(node + " -> " + (status != null ? status : "UNREACHABLE"));
                }
                continue;
            }

            boolean submitted = false;
            for (String node : nodes) {
                String response = sendRpc(node, "SUBMIT_COMMAND\n" + line + "\n");
                if (response == null) {
                    System.out.println(node + " -> unreachable, trying next node...");
                    continue;
                }
                if (response.startsWith("NOT_LEADER")) {
                    System.out.println(node + " -> " + response + ", trying next node...");
                    continue;
                }
                System.out.println(node + " -> " + response);
                submitted = true;
                break;
            }
            if (!submitted) {
                System.out.println("Failed to submit -- no reachable leader found. Is the cluster up?");
            }
        }
    }

    private static String sendRpc(String node, String request) {
        String[] hostPort = node.split(":");
        try (Socket socket = new Socket()) {
            socket.connect(new java.net.InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1])), 300);
            socket.setSoTimeout(2500); // longer timeout here -- SUBMIT can legitimately wait for commit

            OutputStream out = socket.getOutputStream();
            out.write(request.getBytes());
            out.flush();

            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            return in.readLine();
        } catch (IOException e) {
            return null;
        }
    }
}