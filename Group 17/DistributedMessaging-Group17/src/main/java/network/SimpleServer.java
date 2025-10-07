package network;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.function.Consumer;

public class SimpleServer {
    private static final String HEALTH_PING = "PING";
    private static final String HEALTH_PONG = "PONG";

    private final int port;
    private final Consumer<String> messageHandler;
    private ServerSocket serverSocket;
    private volatile boolean running = false;
    private Thread serverThread;
    
    public SimpleServer(int port, Consumer<String> messageHandler) {
        this.port = port;
        this.messageHandler = messageHandler;
    }
    
    public void start() {
        try {
            serverSocket = new ServerSocket(port);
            running = true;
            System.out.println("🌐 Server started on port " + port);
            
            serverThread = new Thread(this::acceptConnections, "server-" + port);
            serverThread.setDaemon(true);
            serverThread.start();
        } catch (IOException e) {
            System.err.println("❌ Failed to start server on port " + port + ": " + e.getMessage());
            throw new RuntimeException("Server startup failed", e);
        }
    }
    
    private void acceptConnections() {
        while (running && !serverSocket.isClosed()) {
            try {
                Socket clientSocket = serverSocket.accept();
                // Do not log at accept; log later only for real app messages.
                Thread clientThread = new Thread(() -> handleClient(clientSocket), "client-" + port);
                clientThread.setDaemon(true);
                clientThread.start();
            } catch (IOException e) {
                if (running) {
                    // When stopping, accept() will throw because serverSocket is closed — ignore then.
                    System.err.println("❌ Error accepting connection: " + e.getMessage());
                }
            }
        }
    }
    
    private void handleClient(Socket clientSocket) {
        try {
            clientSocket.setSoTimeout(5000); // don’t block forever waiting for a line
            try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                 PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

                // Speak a tiny protocol:
                // 1) For app clients, we first send READY, then expect a line (message)
                // 2) For health checks, the client will send PING immediately (no READY expected)
                out.println("READY");

                String line = in.readLine();
                if (line == null) {
                    // client disconnected without saying anything (likely a probe) — stay quiet
                    return;
                }

                // Health-check fast path
                if (HEALTH_PING.equalsIgnoreCase(line.trim())) {
                    out.println(HEALTH_PONG);
                    // Quietly finish — no log spam
                    return;
                }

                // Real app message
                System.out.println("🔗 New client connected: " + clientSocket.getInetAddress());
                System.out.println("📨 Received message: " + line);
                if (messageHandler != null) {
                    messageHandler.accept(line);
                    out.println("ACK");
                }
                
                // Continue processing additional lines until EXIT (optional)
                while ((line = in.readLine()) != null && !"EXIT".equals(line)) {
                    if (HEALTH_PING.equalsIgnoreCase(line.trim())) {
                        out.println(HEALTH_PONG);
                        continue;
                    }
                    System.out.println("📨 Received message: " + line);
                    if (messageHandler != null) {
                        messageHandler.accept(line);
                        out.println("ACK");
                    }
                }
            }
        } catch (SocketTimeoutException ignore) {
            // Idle connection, just drop quietly
        } catch (IOException e) {
            // Suppress noisy messages for normal client resets during probes
            String msg = e.getMessage();
            if (msg != null && (msg.contains("Connection reset") || msg.contains("aborted"))) {
                // Ignore normal half-close/reset from health checks
            } else {
                System.err.println("❌ Error handling client: " + e.getMessage());
            }
        } finally {
            try { clientSocket.close(); } catch (IOException ignore) {}
        }
    }
    
    public void stop() {
        running = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
            if (serverThread != null) {
                serverThread.join(2000);
            }
            System.out.println("🛑 Server stopped on port " + port);
        } catch (Exception e) {
            System.err.println("❌ Error stopping server: " + e.getMessage());
        }
    }
    
    public boolean isRunning() {
        return running;
    }
}
