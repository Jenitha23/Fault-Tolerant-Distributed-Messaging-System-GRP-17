package network;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class TCPClient {

    // Tunables (no VM args needed)
    private static final int CONNECT_TIMEOUT_MS = 1_000;  // connect timeout
    private static final int SO_TIMEOUT_MS      = 10_000; // read timeout

    /**
     * Sends a single line message and waits for "ACK".
     * Protocol: connect -> wait "READY" -> send message -> wait "ACK" -> close
     */
    public static boolean sendMessage(String host, int port, String message) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
            socket.setSoTimeout(SO_TIMEOUT_MS);

            try (PrintWriter out = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8);
                 BufferedReader in = new BufferedReader(
                         new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {

                // Expect handshake
                String ready = in.readLine();
                if (!"READY".equals(ready)) {
                    System.err.println("❌ Server not ready (got: " + ready + ")");
                    return false;
                }

                // Send message
                out.println(message);

                // Wait for ack
                String ack = in.readLine();
                if ("ACK".equals(ack)) {
                    System.out.println("✅ Message delivered to " + host + ":" + port);
                    return true;
                } else {
                    System.err.println("❌ Server did not acknowledge (got: " + ack + ")");
                    return false;
                }
            }
        } catch (IOException e) {
            System.err.println("❌ Failed to send message to " + host + ":" + port + " - " + e.getMessage());
            return false;
        }
    }

    /**
     * Generic request/response helper (still line-based).
     * Fix: now also consumes the "READY" handshake before sending the request.
     */
    public static String sendRequest(String host, int port, String request) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
            socket.setSoTimeout(SO_TIMEOUT_MS);

            try (PrintWriter out = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8);
                 BufferedReader in = new BufferedReader(
                         new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {

                // Consume READY handshake
                String ready = in.readLine();
                if (!"READY".equals(ready)) {
                    System.err.println("❌ Server not ready (got: " + ready + ")");
                    return null;
                }

                // Send request
                out.println(request);

                // Return first response line (caller decides what to do)
                return in.readLine();
            }
        } catch (IOException e) {
            System.err.println("❌ Failed to send request to " + host + ":" + port + " - " + e.getMessage());
            return null;
        }
    }
}
