package com.aditya.kvstore.raft;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;


public class RaftKvClient {

    public static void main(String[] args) throws Exception {
        List<String> clientNodes = Arrays.asList("localhost:8001", "localhost:8002", "localhost:8003");

        System.out.println("Connected to Raft-backed KV cluster: " + clientNodes);
        System.out.println("Type commands like: SET foo bar | GET foo | DEL foo | SETEX foo 30 bar | STATUS");
        System.out.println("Type EXIT to quit.\n");

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("> ");
            String line = scanner.nextLine();
            if (line.equalsIgnoreCase("EXIT")) break;

            String upper = line.toUpperCase();

            if (upper.equals("STATUS")) {

                for (String node : clientNodes) {
                    String response = sendCommand(node, line);
                    System.out.println(node + " -> " + (response != null ? response : "UNREACHABLE"));
                }
                continue;
            }

            boolean isRead = upper.startsWith("GET") || upper.startsWith("TTL");

            if (isRead) {

                boolean done = false;
                for (String node : clientNodes) {
                    String response = sendCommand(node, line);
                    if (response != null) {
                        System.out.println(node + " -> " + response);
                        done = true;
                        break;
                    }
                }
                if (!done) System.out.println("No reachable node found.");
                continue;
            }


            boolean done = false;
            for (String node : clientNodes) {
                String response = sendCommand(node, line);
                if (response == null) {
                    System.out.println(node + " -> unreachable, trying next node...");
                    continue;
                }
                if (response.startsWith("-ERR not leader")) {
                    System.out.println(node + " -> " + response.trim() + ", trying next node...");
                    continue;
                }
                System.out.println(node + " -> " + response);
                done = true;
                break;
            }
            if (!done) System.out.println("Failed to submit -- no reachable leader found.");
        }
    }

    private static String sendCommand(String node, String command) {
        String[] hostPort = node.split(":");
        try (Socket socket = new Socket()) {
            socket.connect(new java.net.InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1])), 300);
            socket.setSoTimeout(2500);

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
}