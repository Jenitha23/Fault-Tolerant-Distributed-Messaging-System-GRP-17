package faulttolerance;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Periodically probes peers via a tiny "PING"/"PONG" handshake on the same port as SimpleServer.
 * Emits events ONLY on state changes, with debounce:
 *  - DOWN after N consecutive failures
 *  - UP after M consecutive successes
 */
public class FaultToleranceManager {

    public interface Listener {
        void onNodeDown(String peer);
        void onNodeUp(String peer);
    }

    // ---- Tunables ----
    private final int checkIntervalMillis  = 3000; // probe every 3s
    private final int connectTimeoutMillis = 500;  // TCP connect timeout
    private final int ioTimeoutMillis      = 800;  // read timeout to receive PONG
    private final int failuresToMarkDown   = 3;    // DOWN after 3 consecutive fails
    private final int successesToMarkUp    = 1;    // UP after 1 success

    private static final String HEALTH_PING = "PING";
    private static final String HEALTH_PONG = "PONG";

    // ---- Internal state ----
    private volatile List<String> peers = List.of();
    private final ConcurrentHashMap<String, Boolean> isUp = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> failStreak = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> okStreak   = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler =
        Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "failure-detector");
            t.setDaemon(true);
            return t;
        });

    private ScheduledFuture<?> task;

    private volatile Listener listener = new Listener() {
        @Override public void onNodeDown(String peer) { System.out.println("💥 Node failure detected: " + peer); }
        @Override public void onNodeUp(String peer)   { System.out.println("✅ Node recovered: " + peer); }
    };

    // ---- Public API ----
    public void setNodes(List<String> peers) {
        this.peers = new ArrayList<>(peers);
        for (String p : peers) {
            isUp.putIfAbsent(p, true); // assume up at start
            failStreak.putIfAbsent(p, new AtomicInteger(0));
            okStreak.putIfAbsent(p, new AtomicInteger(0));
        }
    }

    public void setListener(Listener listener) {
        if (listener != null) this.listener = listener;
    }

    public synchronized void startFailureDetection() {
        if (task != null && !task.isCancelled()) return;
        task = scheduler.scheduleWithFixedDelay(this::checkAll, 0, checkIntervalMillis, TimeUnit.MILLISECONDS);
        System.out.println("🔍 Failure detection started - checking nodes every " + (checkIntervalMillis / 1000) + " seconds");
    }

    public synchronized void stop() {
        if (task != null) task.cancel(true);
        scheduler.shutdownNow();
    }

    // ---- Worker ----
    private void checkAll() {
        for (String peer : peers) {
            boolean reachable = ping(peer);
            boolean currentlyUp = isUp.getOrDefault(peer, true);

            if (reachable) {
                okStreak.get(peer).incrementAndGet();
                failStreak.get(peer).set(0);

                if (!currentlyUp && okStreak.get(peer).get() >= successesToMarkUp) {
                    isUp.put(peer, true);
                    okStreak.get(peer).set(0);
                    safe(() -> listener.onNodeUp(peer));
                }
            } else {
                failStreak.get(peer).incrementAndGet();
                okStreak.get(peer).set(0);

                if (currentlyUp && failStreak.get(peer).get() >= failuresToMarkDown) {
                    isUp.put(peer, false);
                    failStreak.get(peer).set(0);
                    safe(() -> listener.onNodeDown(peer));
                }
            }
        }
    }

    /**
     * Health check handshake:
     *  1) Connect
     *  2) Read optional "READY" (we ignore contents)
     *  3) Send "PING"
     *  4) Expect "PONG"
     */
    private boolean ping(String hostPort) {
        String[] parts = hostPort.split(":");
        if (parts.length != 2) return false;
        String host = parts[0];
        int port;
        try {
            port = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            return false;
        }

        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(host, port), connectTimeoutMillis);
            s.setSoTimeout(ioTimeoutMillis);

            try (PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()))) {

                // Server may send "READY" first — read one line non-blocking with timeout
                String first = in.readLine(); // could be "READY" or null; either is fine

                // Now our health check message
                out.println(HEALTH_PING);

                String reply = in.readLine(); // expect PONG
                return HEALTH_PONG.equalsIgnoreCase(reply != null ? reply.trim() : "");
            }
        } catch (Exception ignored) {
            return false;
        }
    }

    private void safe(Runnable r) {
        try { r.run(); } catch (Throwable t) {
            System.err.println("FailureDetector listener error: " + t);
        }
    }
}
