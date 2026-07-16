package com.aditya.kvstore.client;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Scanner;


public class TestClient {

    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = 6380;

        try (
                Socket socket = new Socket(host, port);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                OutputStream out = socket.getOutputStream();
                Scanner scanner = new Scanner(System.in)
        ) {
            System.out.println("Connected to KV store at " + host + ":" + port);
            System.out.println("Type commands like: SET name aditya | GET name | DEL name");
            System.out.println("Type EXIT to quit.\n");

            while (true) {
                System.out.print("> ");
                String line = scanner.nextLine();

                if (line.equalsIgnoreCase("EXIT")) break;

                out.write((line + "\n").getBytes());
                out.flush();


                String responseLine = in.readLine();
                System.out.println(responseLine);


                if (responseLine != null && responseLine.startsWith("$") && !responseLine.equals("$-1")) {
                    String valueLine = in.readLine();
                    System.out.println(valueLine);
                }
            }
        }
    }
}