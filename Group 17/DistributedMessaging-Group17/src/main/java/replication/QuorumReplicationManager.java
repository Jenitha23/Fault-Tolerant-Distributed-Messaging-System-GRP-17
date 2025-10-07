package replication;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Simple in-memory quorum replication facade for messages.
 * - Simulates writes/reads across N replicas (in-process).
 * - Achieves success when majority (quorum) acknowledges.
 * - Deduplicates by messageId.
 *
 * NOTE: This is a local simulator for demonstration. In a
 * real system, each "replica" would be a remote node.
 */
public class QuorumReplicationManager {

    private final int totalNodes;
    private final int writeQuorum;
    private final int readQuorum;

    // local store = "source of truth" once a write quorum is reached
    private final ConcurrentHashMap<String, String> messageStore = new ConcurrentHashMap<>();

    // per-replica simulated stores (optional realism)
    private final List<ConcurrentHashMap<String, String>> replicaStores;

    // dedup
    private final DeduplicationService deduplicationService = new DeduplicationService();

    // execution (so we don't spam the common pool)
    private final ExecutorService ioPool;

    // tunables (no VM args needed)
    private static final long WRITE_TIMEOUT_MS = 2_000;
    private static final long READ_TIMEOUT_MS  = 2_000;

    public QuorumReplicationManager(int totalNodes) {
        if (totalNodes < 1) throw new IllegalArgumentException("totalNodes must be >= 1");
        this.totalNodes = totalNodes;
        this.writeQuorum = (totalNodes / 2) + 1;
        this.readQuorum  = (totalNodes / 2) + 1;

        // create N simulated replica stores
        this.replicaStores = new ArrayList<>(totalNodes);
        for (int i = 0; i < totalNodes; i++) {
            this.replicaStores.add(new ConcurrentHashMap<>());
        }

        // small fixed thread pool; tasks are tiny
        this.ioPool = Executors.newFixedThreadPool(Math.min(8, Math.max(2, totalNodes)));

        System.out.println("🔄 Quorum config - Write: " + writeQuorum + "/" + totalNodes +
                           ", Read: " + readQuorum + "/" + totalNodes);
    }

    /**
     * Quorum write: succeed when >= writeQuorum replicas ack.
     */
    public boolean writeMessage(String messageId, String content) {
        if (messageId == null || messageId.isBlank() || content == null) {
            System.out.println("❌ Invalid write (null/blank fields)");
            return false;
        }

        if (deduplicationService.isDuplicate(messageId)) {
            System.out.println("⚠️  Duplicate message " + shortId(messageId) + " - skipping");
            return true; // already stored before
        }

        System.out.println("💾 Writing message " + shortId(messageId) + " with quorum " + writeQuorum);

        List<Future<Boolean>> writes = new ArrayList<>(totalNodes);
        for (int i = 0; i < totalNodes; i++) {
            final int idx = i;
            writes.add(ioPool.submit(() -> simulateWriteToReplica(idx, messageId, content)));
        }

        int successCount = waitForAcks(writes, WRITE_TIMEOUT_MS, writeQuorum);
        boolean ok = successCount >= writeQuorum;

        if (ok) {
            // stabilize into main store
            messageStore.put(messageId, content);
            System.out.println("✅ Write successful - " + successCount + "/" + totalNodes + " replicas");
        } else {
            System.out.println("❌ Write failed - only " + successCount + "/" + writeQuorum + " required");
        }
        return ok;
    }

    /**
     * Quorum read: return the majority value among replica responses.
     * If no quorum, returns null.
     */
    public String readMessage(String messageId) {
        if (messageId == null || messageId.isBlank()) return null;

        System.out.println("📖 Reading message " + shortId(messageId) + " with quorum " + readQuorum);

        List<Future<String>> reads = new ArrayList<>(totalNodes);
        for (int i = 0; i < totalNodes; i++) {
            final int idx = i;
            reads.add(ioPool.submit(() -> simulateReadFromReplica(idx, messageId)));
        }

        List<String> results = waitForResults(reads, READ_TIMEOUT_MS, readQuorum);

        if (results.size() >= readQuorum) {
            String value = resolveMajority(results);
            System.out.println("✅ Read successful - " + results.size() + "/" + totalNodes + " replicas");
            return value;
        } else {
            System.out.println("❌ Read failed - only " + results.size() + "/" + readQuorum + " required");
            return null;
        }
    }

    // --- internal helpers ----------------------------------------------------

    private boolean simulateWriteToReplica(int replicaIndex, String messageId, String content) {
        try {
            // simulate network + disk delay
            sleepJitter(40, 160);

            // small failure chance
            if (Math.random() < 0.08) {
                return false;
            }

            replicaStores.get(replicaIndex).put(messageId, content);
            return true;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private String simulateReadFromReplica(int replicaIndex, String messageId) {
        try {
            // simulate network + disk delay
            sleepJitter(25, 120);

            // small miss chance
            if (Math.random() < 0.05) {
                return null;
            }

            String v = replicaStores.get(replicaIndex).get(messageId);
            // if replica lagged behind, fall back to stabilized store (eventual)
            if (v == null) v = messageStore.get(messageId);
            return v;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private static void sleepJitter(long baseMs, long rangeMs) throws InterruptedException {
        long delay = baseMs + (long) (Math.random() * rangeMs);
        Thread.sleep(delay);
    }

    private static String shortId(String id) {
        return (id.length() >= 8) ? id.substring(0, 8) : id;
    }

    private int waitForAcks(List<Future<Boolean>> futures, long timeoutMs, int needed) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        AtomicInteger ok = new AtomicInteger(0);

        for (Future<Boolean> f : futures) {
            long remaining = Math.max(1, deadline - System.currentTimeMillis());
            if (remaining <= 0) break;

            try {
                Boolean v = f.get(remaining, TimeUnit.MILLISECONDS);
                if (Boolean.TRUE.equals(v) && ok.incrementAndGet() >= needed) {
                    break; // quorum reached
                }
            } catch (Exception ignored) {
                // timeout/cancel/failure – treat as a miss
            }
        }
        return ok.get();
    }

    private List<String> waitForResults(List<Future<String>> futures, long timeoutMs, int needed) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        List<String> results = new ArrayList<>(futures.size());

        for (Future<String> f : futures) {
            long remaining = Math.max(1, deadline - System.currentTimeMillis());
            if (remaining <= 0) break;

            try {
                String v = f.get(remaining, TimeUnit.MILLISECONDS);
                if (v != null) {
                    results.add(v);
                    if (results.size() >= needed) break; // enough for quorum decision
                }
            } catch (Exception ignored) {
                // timeout/cancel/failure – skip
            }
        }
        return results;
    }

    /**
     * Return the majority (most frequent non-null) value; ties pick first seen.
     */
    private String resolveMajority(List<String> values) {
        Map<String, Long> counts = values.stream()
                .filter(v -> v != null)
                .collect(Collectors.groupingBy(v -> v, Collectors.counting()));
        return counts.entrySet()
                .stream()
                .max((a, b) -> Long.compare(a.getValue(), b.getValue()))
                .map(Map.Entry::getKey)
                .orElse(null);
    }

    // --- accessors -----------------------------------------------------------

    public int getWriteQuorum() { return writeQuorum; }
    public int getReadQuorum()  { return readQuorum;  }

    /** For tests/metrics: returns number of keys in stabilized store. */
    public int stabilizedCount() { return messageStore.size(); }

    /** Graceful shutdown of internal thread-pool. */
    public void shutdown() {
        ioPool.shutdownNow();
    }
}
