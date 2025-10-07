package timesync;

import common.Message;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple Hybrid Logical Clock (HLC)-style helper used by the node.
 * - Maintains a physical offset (averaged from peers) and a monotonically
 *   increasing logical counter for message ordering within the same millisecond.
 * - No VM args; all defaults are safe.
 */
public class HybridTimeSynchronizer {

    /** Average offset from peers (ms), applied to System.currentTimeMillis(). */
    private volatile long clockOffset = 0L;

    /** Logical time increases on every local event / send / receive. */
    private final AtomicLong logicalTime = new AtomicLong(0);

    /** Max tolerated skew (ms) to warn about clock issues. */
    private static final long MAX_CLOCK_SKEW_MS = 1_000; // 1s

    /**
     * Best-effort “sync”: pretend to query each peer’s time and average offsets.
     * In a real system you’d use NTP/PTP or Cristian’s/Berkeley’s algorithm.
     */
    public void synchronizeClocks(List<String> peerNodes) {
        if (peerNodes == null || peerNodes.isEmpty()) {
            System.out.println("⏰ No peers for clock synchronization - using system time");
            return;
        }

        System.out.println("🕒 Synchronizing clock with " + peerNodes.size() + " peers");

        long totalOffset = 0;
        int successfulSyncs = 0;

        for (String peer : peerNodes) {
            try {
                long peerTime = simulatePeerTimeRequest(peer);
                long localTime = System.currentTimeMillis();
                long offset = peerTime - localTime;
                totalOffset += offset;
                successfulSyncs++;
                System.out.println("📡 Peer " + peer + " offset: " + offset + "ms");
            } catch (Exception e) {
                System.out.println("❌ Clock sync failed with peer " + peer);
            }
        }

        if (successfulSyncs > 0) {
            this.clockOffset = totalOffset / successfulSyncs;
            System.out.println("✅ Clock synchronized - average offset: " + clockOffset + "ms");
        } else {
            System.out.println("⚠️  Could not synchronize with any peers");
        }
    }

    /** Physical time adjusted by the current offset. */
    public long getCurrentTimestamp() {
        return System.currentTimeMillis() + clockOffset;
    }

    /** Next logical counter (monotonic). */
    public long getNextLogicalTime() {
        return logicalTime.incrementAndGet();
    }

    /**
     * Call this when receiving a message that carries a (remotePhysical, remoteLogical).
     * It merges the local logical clock to preserve causal/monotonic ordering.
     */
    public void onReceive(long remotePhysicalTs, long remoteLogicalTs) {
        long localPhysical = getCurrentTimestamp();

        if (remotePhysicalTs > localPhysical) {
            logicalTime.updateAndGet(local -> Math.max(local, remoteLogicalTs) + 1);
        } else if (remotePhysicalTs == localPhysical) {
            logicalTime.updateAndGet(local -> Math.max(local, remoteLogicalTs) + 1);
        } else {
            logicalTime.incrementAndGet();
        }
    }

    /**
     * Returns true if skew is beyond the allowed limit (and logs a warning).
     */
    public boolean detectClockSkew(long remoteTimestamp, String sourceNode) {
        long localTimestamp = getCurrentTimestamp();
        long skew = Math.abs(localTimestamp - remoteTimestamp);

        if (skew > MAX_CLOCK_SKEW_MS) {
            System.out.println("⚠️  Clock skew detected with " + sourceNode + ": " + skew + "ms");
            return true;
        }
        return false;
    }

    /** Overwrite a message’s timestamp (used after skew detection/ordering fixes). */
    public void correctTimestamp(Message message, long correctedTimestamp) {
        long old = message.getTimestamp();
        message.setTimestamp(correctedTimestamp);
        System.out.println("🕒 Corrected timestamp for message " +
                message.getId().substring(0, 8) + " from " + old + " to " + correctedTimestamp);
    }

    // ------------------------------------------------------------------------
    // Demo-only peer time simulation:
    // ------------------------------------------------------------------------
    private long simulatePeerTimeRequest(String peer) throws Exception {
        Thread.sleep(10 + (long) (Math.random() * 50));
        // Return a time within ±100ms of local:
        return System.currentTimeMillis() + (long) (Math.random() * 200 - 100);
    }
}
